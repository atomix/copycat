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

import io.atomix.catalyst.buffer.PooledHeapAllocator;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Managed;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.SingleThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.request.ClientRequestTypeResolver;
import io.atomix.copycat.client.response.ClientResponseTypeResolver;
import io.atomix.copycat.client.session.SessionTypeResolver;
import io.atomix.copycat.server.cluster.Cluster;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.request.ServerRequestTypeResolver;
import io.atomix.copycat.server.response.ServerResponseTypeResolver;
import io.atomix.copycat.server.state.ConnectionManager;
import io.atomix.copycat.server.state.ServerContext;
import io.atomix.copycat.server.state.StateTypeResolver;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.copycat.server.storage.entry.EntryTypeResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

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
 * All serialization is performed with a Catalyst {@link Serializer}.
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
public class CopycatServer implements Managed<CopycatServer> {
  private static final Logger LOGGER = LoggerFactory.getLogger(CopycatServer.class);

  /**
   * Returns a new Raft server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Raft cluster. The local server {@link Address} does
   * not have to be present in the address list.
   *
   * @param address The address through which clients and servers connect to this server.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  public static Builder builder(Address address, Address... cluster) {
    return builder(address, address, Arrays.asList(cluster));
  }

  /**
   * Returns a new Raft server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Raft cluster. The local server {@link Address} does
   * not have to be present in the address list.
   *
   * @param address The address through which clients and servers connect to this server.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  public static Builder builder(Address address, Collection<Address> cluster) {
    return new Builder(address, address, cluster);
  }

  /**
   * Returns a new Raft server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Raft cluster. The local server {@link Address} does
   * not have to be present in the address list.
   *
   * @param type The server member type.
   * @param address The address through which clients and servers connect to this server.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  public static Builder builder(Member.Type type, Address address, Address... cluster) {
    return builder(address, address, Arrays.asList(cluster)).withType(type);
  }

  /**
   * Returns a new Raft server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Raft cluster. The local server {@link Address} does
   * not have to be present in the address list.
   *
   * @param type The server member type.
   * @param address The address through which clients and servers connect to this server.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  public static Builder builder(Member.Type type, Address address, Collection<Address> cluster) {
    return new Builder(address, address, cluster).withType(type);
  }

  /**
   * Returns a new Raft server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Raft cluster. The local server {@link Address} does
   * not have to be present in the address list.
   *
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The local server member address.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  public static Builder builder(Address clientAddress, Address serverAddress, Address... cluster) {
    return builder(clientAddress, serverAddress, Arrays.asList(cluster));
  }

  /**
   * Returns a new Raft server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Raft cluster. The local server {@link Address} does
   * not have to be present in the address list.
   *
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The local server member address.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  public static Builder builder(Address clientAddress, Address serverAddress, Collection<Address> cluster) {
    return new Builder(clientAddress, serverAddress, cluster);
  }

  /**
   * Returns a new Raft server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Raft cluster. The local server {@link Address} does
   * not have to be present in the address list.
   *
   * @param type The server member type.
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The local server member address.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  public static Builder builder(Member.Type type, Address clientAddress, Address serverAddress, Address... cluster) {
    return builder(clientAddress, serverAddress, Arrays.asList(cluster)).withType(type);
  }

  /**
   * Returns a new Raft server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Raft cluster. The local server {@link Address} does
   * not have to be present in the address list.
   *
   * @param type The server member type.
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The local server member address.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  public static Builder builder(Member.Type type, Address clientAddress, Address serverAddress, Collection<Address> cluster) {
    return new Builder(clientAddress, serverAddress, cluster).withType(type);
  }

  /**
   * Raft server state types.
   * <p>
   * States represent the context of the server's internal state machine. Throughout the lifetime of a server,
   * the server will periodically transition between states based on requests, responses, and timeouts.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public enum State {

    /**
     * Represents the state of an inactive server.
     * <p>
     * All servers start in this state and return to this state when {@link #close() stopped}.
     */
    INACTIVE,

    /**
     * Represents the state of a server that is a reserve member of the cluster.
     * <p>
     * Reserve servers only receive notification of leader, term, and configuration changes.
     */
    RESERVE,

    /**
     * Represents the state of a server in the process of catching up its log.
     * <p>
     * Upon successfully joining an existing cluster, the server will transition to the passive state and remain there
     * until the leader determines that the server has caught up enough to be promoted to a full member.
     */
    PASSIVE,

    /**
     * Represents the state of a server participating in normal log replication.
     * <p>
     * The follower state is a standard Raft state in which the server receives replicated log entries from the leader.
     */
    FOLLOWER,

    /**
     * Represents the state of a server attempting to become the leader.
     * <p>
     * When a server in the follower state fails to receive communication from a valid leader for some time period,
     * the follower will transition to the candidate state. During this period, the candidate requests votes from
     * each of the other servers in the cluster. If the candidate wins the election by receiving votes from a majority
     * of the cluster, it will transition to the leader state.
     */
    CANDIDATE,

    /**
     * Represents the state of a server which is actively coordinating and replicating logs with other servers.
     * <p>
     * Leaders are responsible for handling and replicating writes from clients. Note that more than one leader can
     * exist at any given time, but Raft guarantees that no two leaders will exist for the same {@link Cluster#term()}.
     */
    LEADER

  }

  private final String name;
  private final Transport clientTransport;
  private final Transport serverTransport;
  private final Server clientServer;
  private final Server internalServer;
  private final ServerContext context;
  private volatile CompletableFuture<CopycatServer> openFuture;
  private volatile CompletableFuture<Void> closeFuture;
  private Listener<Member> electionListener;
  private boolean open;

  private CopycatServer(String name, Transport clientTransport, Transport serverTransport, ServerContext context) {
    this.name = Assert.notNull(name, "name");
    this.clientTransport = Assert.notNull(clientTransport, "clientTransport");
    this.serverTransport = Assert.notNull(serverTransport, "serverTransport");
    this.internalServer = serverTransport.server();
    this.clientServer = !context.getCluster().member().serverAddress().equals(context.getCluster().member().clientAddress()) ? clientTransport.server() : null;
    this.context = Assert.notNull(context, "context");
  }

  /**
   * Returns the server name.
   *
   * @return The server name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the server storage.
   *
   * @return The server storage.
   */
  public Storage storage() {
    return context.getStorage();
  }

  /**
   * Returns the server's cluster configuration.
   *
   * @return The server's cluster configuration.
   */
  public Cluster cluster() {
    return context.getCluster();
  }

  /**
   * Returns the server serializer.
   *
   * @return The server serializer.
   */
  public Serializer serializer() {
    return context.getSerializer();
  }

  /**
   * Returns the Copycat server state.
   * <p>
   * The initial state of a Raft server is {@link State#INACTIVE}. Once the server is {@link #open() started} and
   * until it is explicitly shutdown, the server will be in one of the active states - {@link State#PASSIVE},
   * {@link State#FOLLOWER}, {@link State#CANDIDATE}, or {@link State#LEADER}.
   *
   * @return The Copycat server state.
   */
  public State state() {
    return context.getState();
  }

  /**
   * Registers a state change listener.
   * <p>
   * Throughout the lifetime of the cluster, the server will periodically transition between various {@link CopycatServer.State states}.
   * Users can listen for and react to state change events. To determine when this server is elected leader, simply
   * listen for the {@link CopycatServer.State#LEADER} state.
   * <pre>
   *   {@code
   *   server.onStateChange(state -> {
   *     if (state == CopycatServer.State.LEADER) {
   *       System.out.println("Server elected leader!");
   *     }
   *   });
   *   }
   * </pre>
   *
   * @param listener The state change listener.
   * @return The listener context. This can be used to unregister the election listener via
   * {@link Listener#close()}.
   * @throws NullPointerException If {@code listener} is {@code null}
   */
  public Listener<State> onStateChange(Consumer<State> listener) {
    return context.onStateChange(listener);
  }

  /**
   * Returns the server execution context.
   * <p>
   * The thread context is the event loop that this server uses to communicate other Raft servers.
   * Implementations must guarantee that all asynchronous {@link java.util.concurrent.CompletableFuture} callbacks are
   * executed on a single thread via the returned {@link io.atomix.catalyst.util.concurrent.ThreadContext}.
   * <p>
   * The {@link io.atomix.catalyst.util.concurrent.ThreadContext} can also be used to access the Raft server's internal
   * {@link io.atomix.catalyst.serializer.Serializer serializer} via {@link ThreadContext#serializer()}. Catalyst serializers
   * are not thread safe, so to use the context serializer, users should clone it:
   * <pre>
   *   {@code
   *   Serializer serializer = server.threadContext().serializer().clone();
   *   Buffer buffer = serializer.writeObject(myObject).flip();
   *   }
   * </pre>
   *
   * @return The server thread context.
   */
  public ThreadContext context() {
    return context.getThreadContext();
  }

  /**
   * Starts the Raft server asynchronously.
   * <p>
   * When the server is started, the server will attempt to search for an existing cluster by contacting all of
   * the members in the provided members list. If no existing cluster is found, the server will immediately transition
   * to the {@link State#FOLLOWER} state and continue normal Raft protocol operations. If a cluster is found, the server
   * will attempt to join the cluster. Once the server has joined or started a cluster and a leader has been found,
   * the returned {@link CompletableFuture} will be completed.
   *
   * @return A completable future to be completed once the server has joined the cluster and a leader has been found.
   */
  @Override
  public CompletableFuture<CopycatServer> open() {
    if (open)
      return CompletableFuture.completedFuture(this);

    if (openFuture == null) {
      synchronized (this) {
        if (openFuture == null) {
          Function<Void, CompletionStage<CopycatServer>> completionFunction = state -> {
            CompletableFuture<CopycatServer> future = new CompletableFuture<>();
            openFuture = null;
            cluster().join().whenComplete((result, error) -> {
              if (error == null) {
                if (cluster().leader() != null) {
                  open = true;
                  future.complete(this);
                } else {
                  electionListener = cluster().onLeaderElection(leader -> {
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
            openFuture = listen().thenCompose(completionFunction);
          } else {
            openFuture = closeFuture.thenCompose(c -> listen().thenCompose(completionFunction));
          }
        }
      }
    }

    return openFuture.whenComplete((result, error) -> {
      if (error == null) {
        LOGGER.info("Server started successfully!");
      } else {
        LOGGER.warn("Failed to start server!");
      }
    });
  }

  /**
   * Starts listening the server.
   */
  private CompletableFuture<Void> listen() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.getThreadContext().executor().execute(() -> {
      internalServer.listen(cluster().member().serverAddress(), c -> context.connectServer(c)).whenComplete((internalResult, internalError) -> {
        if (internalError == null) {
          // If the client address is different than the server address, start a separate client server.
          if (clientServer != null) {
            clientServer.listen(cluster().member().clientAddress(), c -> context.connectClient(c)).whenComplete((clientResult, clientError) -> {
              open = true;
              future.complete(null);
            });
          } else {
            open = true;
            future.complete(null);
          }
        } else {
          future.completeExceptionally(internalError);
        }
      });
    });

    return future;
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
            closeFuture = cluster().leave().thenCompose(v -> kill());
          } else {
            closeFuture = openFuture.thenCompose(c -> cluster().leave().thenCompose(v -> kill()));
          }
        }
      }
    }
    return closeFuture;
  }

  /**
   * Returns a boolean value indicating whether the server is running.
   * <p>
   * Once {@link #open()} is called and the returned {@link CompletableFuture} is completed (meaning this server found
   * a cluster leader), this method will return {@code true} until closed.
   *
   * @return Indicates whether the server is running.
   */
  @Override
  public boolean isClosed() {
    return !open;
  }

  /**
   * Kills the server without leaving the cluster.
   *
   * @return A completable future to be completed once the server has been killed.
   */
  public CompletableFuture<Void> kill() {
    if (!open)
      return Futures.exceptionalFuture(new IllegalStateException("context not open"));

    CompletableFuture<Void> future = new CompletableFuture<>();
    context.getThreadContext().executor().execute(() -> {
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
          }, context.getThreadContext().executor());
        }, context.getThreadContext().executor());
      } else {
        internalServer.close().whenCompleteAsync((internalResult, internalError) -> {
          if (internalError != null) {
            future.completeExceptionally(internalError);
          } else {
            future.complete(null);
          }
        }, context.getThreadContext().executor());
      }

      context.transition(CopycatServer.State.INACTIVE);
    });

    return future.whenCompleteAsync((result, error) -> {
      clientTransport.close();
      serverTransport.close();
      context.close();
      open = false;
    });
  }

  /**
   * Deletes the server and its logs.
   *
   * @return A completable future to be completed once the server has been deleted.
   */
  public CompletableFuture<Void> delete() {
    return close().thenRun(context::delete);
  }

  /**
   * Raft server builder.
   */
  public static class Builder extends io.atomix.catalyst.util.Builder<CopycatServer> {
    private static final String DEFAULT_NAME = "copycat";
    private static final Duration DEFAULT_ELECTION_TIMEOUT = Duration.ofMillis(750);
    private static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofMillis(250);
    private static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofMillis(5000);

    private String name = DEFAULT_NAME;
    private Member.Type type = Member.Type.ACTIVE;
    private Transport clientTransport;
    private Transport serverTransport;
    private Storage storage;
    private Serializer serializer;
    private Supplier<StateMachine> stateMachineFactory;
    private Address clientAddress;
    private Address serverAddress;
    private Set<Address> cluster;
    private Duration electionTimeout = DEFAULT_ELECTION_TIMEOUT;
    private Duration heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    private Duration sessionTimeout = DEFAULT_SESSION_TIMEOUT;

    private Builder(Address clientAddress, Address serverAddress, Collection<Address> cluster) {
      this.clientAddress = Assert.notNull(clientAddress, "clientAddress");
      this.serverAddress = Assert.notNull(serverAddress, "serverAddress");
      this.cluster = new HashSet<>(Assert.notNull(cluster, "cluster"));
      this.type = cluster.contains(serverAddress) ? Member.Type.ACTIVE : Member.Type.RESERVE;
    }

    /**
     * Sets the server name.
     * <p>
     * The server name is used to
     *
     * @param name The server name.
     * @return The server builder.
     */
    public Builder withName(String name) {
      this.name = Assert.notNull(name, "name");
      return this;
    }

    /**
     * Sets the initial server member type.
     *
     * @param type The initial server member type.
     * @return The server builder.
     */
    public Builder withType(Member.Type type) {
      this.type = Assert.notNull(type, "type");
      return this;
    }

    /**
     * Sets the client and server transport.
     *
     * @param transport The client and server transport.
     * @return The server builder.
     * @throws NullPointerException if {@code transport} is null
     */
    public Builder withTransport(Transport transport) {
      Assert.notNull(transport, "transport");
      this.clientTransport = transport;
      this.serverTransport = transport;
      return this;
    }

    /**
     * Sets the client transport.
     *
     * @param transport The client transport.
     * @return The server builder.
     * @throws NullPointerException if {@code transport} is null
     */
    public Builder withClientTransport(Transport transport) {
      this.clientTransport = Assert.notNull(transport, "transport");
      return this;
    }

    /**
     * Sets the server transport.
     *
     * @param transport The server transport.
     * @return The server builder.
     * @throws NullPointerException if {@code transport} is null
     */
    public Builder withServerTransport(Transport transport) {
      this.serverTransport = Assert.notNull(transport, "transport");
      return this;
    }

    /**
     * Sets the Raft serializer.
     *
     * @param serializer The Raft serializer.
     * @return The Raft builder.
     * @throws NullPointerException if {@code serializer} is null
     */
    public Builder withSerializer(Serializer serializer) {
      this.serializer = Assert.notNull(serializer, "serializer");
      return this;
    }

    /**
     * Sets the storage module.
     *
     * @param storage The storage module.
     * @return The Raft server builder.
     * @throws NullPointerException if {@code storage} is null
     */
    public Builder withStorage(Storage storage) {
      this.storage = Assert.notNull(storage, "storage");
      return this;
    }

    /**
     * Sets the Raft state machine factory.
     *
     * @param factory The Raft state machine factory.
     * @return The server builder.
     * @throws NullPointerException if the {@code factory} is {@code null}
     */
    public Builder withStateMachine(Supplier<StateMachine> factory) {
      this.stateMachineFactory = Assert.notNull(factory, "factory");
      return this;
    }

    /**
     * Sets the Raft election timeout, returning the Raft configuration for method chaining.
     *
     * @param electionTimeout The Raft election timeout duration.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the election timeout is not positive
     * @throws NullPointerException if {@code electionTimeout} is null
     */
    public Builder withElectionTimeout(Duration electionTimeout) {
      Assert.argNot(electionTimeout.isNegative() || electionTimeout.isZero(), "electionTimeout must be positive");
      Assert.argNot(electionTimeout.toMillis() <= heartbeatInterval.toMillis(), "electionTimeout must be greater than heartbeatInterval");
      this.electionTimeout = Assert.notNull(electionTimeout, "electionTimeout");
      return this;
    }

    /**
     * Sets the Raft heartbeat interval, returning the Raft configuration for method chaining.
     *
     * @param heartbeatInterval The Raft heartbeat interval duration.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the heartbeat interval is not positive
     * @throws NullPointerException if {@code heartbeatInterval} is null
     */
    public Builder withHeartbeatInterval(Duration heartbeatInterval) {
      Assert.argNot(heartbeatInterval.isNegative() || heartbeatInterval.isZero(), "sessionTimeout must be positive");
      Assert.argNot(heartbeatInterval.toMillis() >= electionTimeout.toMillis(), "heartbeatInterval must be less than electionTimeout");
      this.heartbeatInterval = Assert.notNull(heartbeatInterval, "heartbeatInterval");
      return this;
    }

    /**
     * Sets the Raft session timeout, returning the Raft configuration for method chaining.
     *
     * @param sessionTimeout The Raft session timeout duration.
     * @return The Raft configuration.
     * @throws IllegalArgumentException If the session timeout is not positive
     * @throws NullPointerException if {@code sessionTimeout} is null
     */
    public Builder withSessionTimeout(Duration sessionTimeout) {
      Assert.argNot(sessionTimeout.isNegative() || sessionTimeout.isZero(), "sessionTimeout must be positive");
      Assert.argNot(sessionTimeout.toMillis() <= electionTimeout.toMillis(), "sessionTimeout must be greater than electionTimeout");
      this.sessionTimeout = Assert.notNull(sessionTimeout, "sessionTimeout");
      return this;
    }

    /**
     * @throws ConfigurationException if a state machine, members or transport are not configured
     */
    @Override
    public CopycatServer build() {
      if (stateMachineFactory == null)
        throw new ConfigurationException("state machine not configured");

      // If the transport is not configured, attempt to use the default Netty transport.
      if (serverTransport == null) {
        try {
          serverTransport = (Transport) Class.forName("io.atomix.catalyst.transport.NettyTransport").newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
          throw new ConfigurationException("transport not configured");
        }
      }

      // If the client transport is not configured, default it to the server transport.
      if (clientTransport == null) {
        clientTransport = serverTransport;
      }

      // If no serializer instance was provided, create one.
      if (serializer == null) {
        serializer = new Serializer(new PooledHeapAllocator());
      }

      // Resolve serializable request/response and other types.
      serializer.resolve(new ClientRequestTypeResolver());
      serializer.resolve(new ClientResponseTypeResolver());
      serializer.resolve(new SessionTypeResolver());
      serializer.resolve(new ServerRequestTypeResolver());
      serializer.resolve(new ServerResponseTypeResolver());
      serializer.resolve(new EntryTypeResolver());
      serializer.resolve(new StateTypeResolver());

      // If the storage is not configured, create a new Storage instance with the configured serializer.
      if (storage == null) {
        storage = new Storage();
      }

      ConnectionManager connections = new ConnectionManager(serverTransport.client());
      ThreadContext threadContext = new SingleThreadContext("copycat-server-" + serverAddress, serializer);

      ServerContext context = new ServerContext(name, type, serverAddress, clientAddress, cluster, storage, serializer, stateMachineFactory, connections, threadContext);
      context.setElectionTimeout(electionTimeout)
        .setHeartbeatInterval(heartbeatInterval)
        .setSessionTimeout(sessionTimeout);

      return new CopycatServer(name, clientTransport, serverTransport, context);
    }
  }

}
