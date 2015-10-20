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
  private final Address address;
  private final Collection<Address> members;
  private final StateMachine userStateMachine;
  private final Transport transport;
  private final Storage storage;
  private final ThreadContext context;
  private Server server;
  private ServerState state;
  private volatile boolean open;

  public ServerContext(Address address, Collection<Address> members, StateMachine stateMachine, Transport transport, Storage storage, Serializer serializer) {
    this.address = address;
    this.members = members;
    this.userStateMachine = stateMachine;
    this.transport = transport;
    this.storage = storage;
    this.context = new SingleThreadContext("copycat-server-" + address, serializer);

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
      server = transport.server();
      ConnectionManager connections = new ConnectionManager(transport.client());

      server.listen(address, c -> state.connect(c)).thenRun(() -> {
        state = new ServerState(address, members, log, userStateMachine, connections, context);
        open = true;
        future.complete(state);
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
      server.close().whenCompleteAsync((result, error) -> {
        context.close();
        future.complete(null);
      }, context.executor());

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
    return CompletableFuture.runAsync(() -> {
      storage.open("copycat").delete();
    }, context.executor());
  }

}
