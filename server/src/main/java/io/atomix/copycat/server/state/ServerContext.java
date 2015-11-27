/*
 * Copyright 2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.copycat.server.state;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.ServiceLoaderTypeResolver;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Managed;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.SingleThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.server.RaftServer;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Server context.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ServerContext implements Managed<ServerState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerContext.class);
  private final Address clientAddress;
  private final Address serverAddress;
  private final Collection<Address> members;
  private final StateMachine userStateMachine;
  private final Transport clientTransport;
  private final Transport serverTransport;
  private final Storage storage;
  private final ThreadContext context;
  private Server clientServer;
  private Server internalServer;
  private ServerState state;
  private volatile boolean open;

  public ServerContext(Address clientAddress, Transport clientTransport, Address serverAddress, Transport serverTransport, Collection<Address> members, StateMachine stateMachine, Storage storage, Serializer serializer) {
    this.clientAddress = Assert.notNull(clientAddress, "clientAddress");
    this.serverAddress = Assert.notNull(serverAddress, "serverAddress");
    this.clientTransport = Assert.notNull(clientTransport, "clientTransport");
    this.serverTransport = Assert.notNull(serverTransport, "serverTransport");
    this.members = Assert.notNull(members, "members");
    this.userStateMachine = Assert.notNull(stateMachine, "stateMachine");
    this.storage = Assert.notNull(storage, "storage");
    this.context = new SingleThreadContext("copycat-server-" + serverAddress, serializer);

    storage.serializer().resolve(new ServiceLoaderTypeResolver());
    serializer.resolve(new ServiceLoaderTypeResolver());
  }

  @Override
  public CompletableFuture<ServerState> open() {
    CompletableFuture<ServerState> future = new CompletableFuture<>();
    context.executor().execute(() -> {

      // Open the log.
      Log log = storage.open("copycat");

      // Setup the server and connection manager.
      internalServer = serverTransport.server();
      ConnectionManager connections = new ConnectionManager(serverTransport.client());

      internalServer.listen(serverAddress, c -> state.connectServer(c)).whenComplete((internalResult, internalError) -> {
        if (internalError == null) {
          state = new ServerState(new Member(serverAddress, clientAddress), members, log, userStateMachine, connections, context);

          // If the client address is different than the server address, start a separate client server.
          if (!clientAddress.equals(serverAddress)) {
            clientServer = clientTransport.server();
            clientServer.listen(clientAddress, c -> state.connectClient(c)).whenComplete((clientResult, clientError) -> {
              open = true;
              future.complete(state);
            });
          } else {
            open = true;
            future.complete(state);
          }
        } else {
          future.completeExceptionally(internalError);
        }
      });
    });

    return future.whenComplete((result, error) -> {
      if (error == null) {
        LOGGER.info("Server started successfully!");
      } else {
        LOGGER.warn("Failed to start server!");
      }
    });
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!open)
      return Futures.exceptionalFuture(new IllegalStateException("context not open"));

    CompletableFuture<Void> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      open = false;
      if (clientServer != null) {
        clientServer.close().whenCompleteAsync((clientResult, clientError) -> {
          internalServer.close().whenCompleteAsync((internalResult, internalError) -> {
            if (internalError != null) {
              future.completeExceptionally(internalError);
            } else if (clientError != null) {
              future.completeExceptionally(clientError);
            } else {
              future.complete(null);
            }
            context.close();
          }, context.executor());
        }, context.executor());
      } else {
        internalServer.close().whenCompleteAsync((internalResult, internalError) -> {
          if (internalError != null) {
            future.completeExceptionally(internalError);
          } else {
            future.complete(null);
          }
          context.close();
        }, context.executor());
      }

      this.state.transition(RaftServer.State.INACTIVE);
      try {
        this.state.getLog().close();
      } catch (Exception e) {
      }
      this.state.getStateMachine().close();
      this.state = null;
    });
    return future;
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  /**
   * Deletes the context.
   */
  public CompletableFuture<Void> delete() {
    if (open)
      return Futures.exceptionalFuture(new IllegalStateException("cannot delete open context"));
    Log log = storage.open("copycat");
    log.close();
    log.delete();
    return CompletableFuture.completedFuture(null);
  }

}
