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
package io.atomix.copycat.server;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.ServiceLoaderTypeResolver;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.server.state.Member;
import io.atomix.copycat.server.state.ServerContext;
import io.atomix.copycat.server.state.ServerState;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Provides a standalone implementation of the <a href="http://raft.github.io/">Raft consensus algorithm</a>.
 * <p>
 * To create a new server, use the server {@link CopycatServer.Builder}. Servers require
 * cluster membership information in order to perform communication. Each server must be provided a local {@link Address}
 * to which to bind the internal {@link io.atomix.catalyst.transport.Server} and a set of addresses for other members in
 * the cluster.
 * <p>
 * Underlying each server is a {@link StateMachine}. The state machine is responsible for maintaining the state with
 * relation to {@link Command}s and {@link Query}s submitted to the
 * server by a client.
 * <pre>
 *   {@code
 *   Address address = new Address("123.456.789.0", 5000);
 *   Collection<Address> members = Arrays.asList(new Address("123.456.789.1", 5000), new Address("123.456.789.2", 5000));
 *
 *   CopycatServer server = CopycatServer.builder(address, members)
 *     .withStateMachine(new MyStateMachine())
 *     .build();
 *   }
 * </pre>
 * Server state machines are responsible for registering {@link Command}s which can be submitted
 * to the cluster. Raft relies upon determinism to ensure consistency throughout the cluster, so <em>it is imperative
 * that each server in a cluster have the same state machine with the same commands.</em>
 * <p>
 * By default, the server will use the {@code NettyTransport} for communication. You can configure the transport via
 * {@link CopycatServer.Builder#withTransport(Transport)}.
 * <p>
 * As {@link Command}s are received by the server, they're written to the Raft {@link Log}
 * and replicated to other members of the cluster. By default, the log is stored on disk, but users can override the default
 * {@link Storage} configuration via {@link CopycatServer.Builder#withStorage(Storage)}. Most notably,
 * to configure the storage module to store entries in memory instead of disk, configure the
 * {@link StorageLevel}.
 * <pre>
 * {@code
 * CopycatServer server = CopycatServer.builder(address, members)
 *   .withStateMachine(new MyStateMachine())
 *   .withStorage(new Storage(StorageLevel.MEMORY))
 *   .build();
 * }
 * </pre>
 * All serialization is performed with a Catalyst {@link Serializer}. By default, the serializer loads registered
 * {@link io.atomix.catalyst.serializer.CatalystSerializable} types with {@link ServiceLoaderTypeResolver}, but users
 * can provide a custom serializer via {@link CopycatServer.Builder#withSerializer(Serializer)}.
 * The server will still ensure that internal serializable types are properly registered on user-provided serializers.
 * <p>
 * Once the server has been created, to connect to a cluster simply {@link #open()} the server. The server API is
 * fully asynchronous and relies on {@link CompletableFuture} to provide promises:
 * <pre>
 * {@code
 * server.open().thenRun(() -> {
 *   System.out.println("Server started successfully!");
 * });
 * }
 * </pre>
 * When the server is started, it will attempt to connect to an existing cluster. If the server cannot find any
 * existing members, it will attempt to form its own cluster.
 * <p>
 * Once the server is started, it will communicate with the rest of the nodes in the cluster, periodically
 * transitioning between states. Users can listen for state transitions via {@link #onStateChange(Consumer)}:
 * <pre>
 * {@code
 * server.onStateChange(state -> {
 *   if (state == CopycatServer.State.LEADER) {
 *     System.out.println("Server elected leader!");
 *   }
 * });
 * }
 * </pre>
 *
 * @see StateMachine
 * @see Transport
 * @see Storage
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatRaftServer implements CopycatServer {
  private final ServerContext context;
  private CompletableFuture<CopycatServer> openFuture;
  private CompletableFuture<Void> closeFuture;
  private ServerState state;
  private final Duration electionTimeout;
  private final Duration heartbeatInterval;
  private final Duration sessionTimeout;
  private Listener<Address> electionListener;
  private boolean open;

  protected CopycatRaftServer(ServerContext context, Duration electionTimeout, Duration heartbeatInterval, Duration sessionTimeout) {
    this.context = context;
    this.electionTimeout = electionTimeout;
    this.heartbeatInterval = heartbeatInterval;
    this.sessionTimeout = sessionTimeout;
  }

  @Override
  public long term() {
    Assert.state(isOpen(), "server not open");
    return state.getTerm();
  }

  @Override
  public Address leader() {
    Assert.state(isOpen(), "server not open");
    return state.getLeader().serverAddress();
  }

  @Override
  public Listener<Address> onLeaderElection(Consumer<Address> listener) {
    Assert.state(isOpen(), "server not open");
    return state.onLeaderElection(listener);
  }

  @Override
  public Collection<Address> members() {
    Assert.state(isOpen(), "server not open");
    return state.getMembers().stream().map(Member::serverAddress).collect(Collectors.toList());
  }

  @Override
  public State state() {
    Assert.state(isOpen(), "server not open");
    return state.getState();
  }

  @Override
  public Listener<State> onStateChange(Consumer<State> listener) {
    Assert.state(isOpen(), "server not open");
    return state.onStateChange(listener);
  }

  @Override
  public ThreadContext context() {
    Assert.state(isOpen(), "server not open");
    return state.getThreadContext();
  }

  @Override
  public CompletableFuture<CopycatServer> open() {
    if (open)
      return CompletableFuture.completedFuture(this);

    if (openFuture == null) {
      synchronized (this) {
        if (openFuture == null) {
          Function<ServerState, CompletionStage<CopycatServer>> completionFunction = state -> {
            CompletableFuture<CopycatServer> future = new CompletableFuture<>();
            openFuture = null;
            this.state = state;
            state.setElectionTimeout(electionTimeout)
              .setHeartbeatInterval(heartbeatInterval)
              .setSessionTimeout(sessionTimeout)
              .join()
              .whenComplete((result, error) -> {
                if (error == null) {
                  if (state.getLeader() != null) {
                    open = true;
                    future.complete(this);
                  } else {
                    electionListener = state.onLeaderElection(leader -> {
                      if (electionListener != null) {
                        open = true;
                        future.complete(this);
                        electionListener.close();
                        electionListener = null;
                      }
                    });
                  }
                } else {
                  future.completeExceptionally(error);
                }
              });
            return future;
          };

          if (closeFuture == null) {
            openFuture = context.open().thenCompose(completionFunction);
          } else {
            openFuture = closeFuture.thenCompose(c -> context.open().thenCompose(completionFunction));
          }
        }
      }
    }
    return openFuture;
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (!open)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          if (openFuture == null) {
            closeFuture = state.leave()
              .thenCompose(v -> context.close())
              .whenComplete((result, error) -> state = null);
          } else {
            closeFuture = openFuture.thenCompose(c -> state.leave()
              .thenCompose(v -> context.close()))
              .whenComplete((result, error) -> state = null);
          }
        }
      }
    }

    return closeFuture.whenComplete((result, error) -> open = false);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  @Override
  public CompletableFuture<Void> delete() {
    return close().thenRun(context::delete);
  }

}
