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

import io.atomix.catalyst.buffer.PooledDirectAllocator;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.ServiceLoaderTypeResolver;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Managed;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.server.state.ServerContext;
import io.atomix.copycat.server.state.ServerState;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;

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
 *   RaftServer server = RaftServer.builder(address, members)
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
 * RaftServer server = RaftServer.builder(address, members)
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
 *   if (state == RaftServer.State.LEADER) {
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
     * All servers start in this state and return to this state when {@link CopycatServer#close() stopped}.
     */
    INACTIVE,

    /**
     * Represents the state of a server attempting to join a cluster.
     * <p>
     * When a server is {@link CopycatServer#open() started}, the first state to which it transitions is the join state.
     * During that period, the server will attempt to join an existing cluster.
     */
    JOIN,

    /**
     * Represents the state of a server attempting to leave a cluster.
     * <p>
     * When a server is {@link CopycatServer#close() stopped}, the server will first transition to the leave state and
     * remove itself from the cluster configuration during this period.
     */
    LEAVE,

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
     * exist at any given time, but Raft guarantees that no two leaders will exist for the same {@link CopycatServer#term()}.
     */
    LEADER

  }

  /**
   * Returns a new Raft server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Raft cluster. The local server {@link Address} does
   * not have to be present in the address list.
   * <p>
   * The returned server builder will use the {@code NettyTransport} by default. Additionally, serializable types will
   * be registered using the {@link ServiceLoaderTypeResolver}. To register serializable types for the server, simply
   * add a {@link io.atomix.catalyst.serializer.Serializer} or {@link io.atomix.catalyst.serializer.CatalystSerializable}
   * file to your {@code META-INF/services} folder on the classpath.
   *
   * @param address The local server member ID. This must be the ID of a member listed in the provided members list.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  public static Builder builder(Address address, Address... cluster) {
    return builder(address, Arrays.asList(cluster));
  }

  /**
   * Returns a new Raft server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Raft cluster. The local server {@link Address} does
   * not have to be present in the address list.
   * <p>
   * The returned server builder will use the {@code NettyTransport} by default. Additionally, serializable types will
   * be registered using the {@link ServiceLoaderTypeResolver}. To register serializable types for the server, simply
   * add a {@link io.atomix.catalyst.serializer.Serializer} or {@link io.atomix.catalyst.serializer.CatalystSerializable}
   * file to your {@code META-INF/services} folder on the classpath.
   *
   * @param address The local server member ID. This must be the ID of a member listed in the provided members list.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  public static Builder builder(Address address, Collection<Address> cluster) {
    return new Builder(address, cluster);
  }

  private final ServerContext context;
  private CompletableFuture<CopycatServer> openFuture;
  private CompletableFuture<Void> closeFuture;
  private ServerState state;
  private final Duration electionTimeout;
  private final Duration heartbeatInterval;
  private final Duration sessionTimeout;
  private Listener<Address> electionListener;
  private boolean open;

  private CopycatServer(ServerContext context, Duration electionTimeout, Duration heartbeatInterval, Duration sessionTimeout) {
    this.context = context;
    this.electionTimeout = electionTimeout;
    this.heartbeatInterval = heartbeatInterval;
    this.sessionTimeout = sessionTimeout;
  }

  /**
   * Returns the current Raft term.
   * <p>
   * The term is a monotonically increasing number that essentially acts as a logical time for the cluster. For any
   * given term, Raft guarantees that only one {@link #leader()} can be elected, but note that a leader may also
   * not yet exist for the term.
   *
   * @return The current Raft term.
   */
  public long term() {
    return state.getTerm();
  }

  /**
   * Returns the current Raft leader.
   * <p>
   * If no leader has been elected, the leader address will be {@code null}.
   *
   * @return The current Raft leader or {@code null} if this server does not know of any leader.
   */
  public Address leader() {
    return state.getLeader();
  }

  /**
   * Registers a leader election listener.
   * <p>
   * The provided {@link Consumer} will be called whenever a new leader is elected. Note that this can
   * happen repeatedly throughout the lifetime of the cluster. Raft guarantees that no two leaders can
   * be elected for the same {@link #term()}, but that does not necessarily mean that another server
   * cannot believe another node to be the leader.
   *
   * @param listener The leader election listener.
   * @return The listener context. This can be used to unregister the election listener via
   * {@link Listener#close()}.
   * @throws NullPointerException If {@code listener} is {@code null}
   */
  public Listener<Address> onLeaderElection(Consumer<Address> listener) {
    return state.onLeaderElection(listener);
  }

  /**
   * Returns the Raft server state.
   *
   * @return The Raft server state.
   */
  public State state() {
    return state.getState();
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
   *     if (state == RaftServer.State.LEADER) {
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
    return state.onStateChange(listener);
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
   * @return The Raft context.
   */
  public ThreadContext threadContext() {
    return state.getThreadContext();
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
          Function<ServerState, CompletionStage<CopycatServer>> completionFunction = state -> {
            openFuture = null;
            this.state = state;
            state.setElectionTimeout(electionTimeout)
              .setHeartbeatInterval(heartbeatInterval)
              .setSessionTimeout(sessionTimeout)
              .transition(State.JOIN).join();

            if (state.getLeader() != null) {
              return CompletableFuture.completedFuture(this);
            } else {
              CompletableFuture<CopycatServer> future = new CompletableFuture<>();
              electionListener = state.onLeaderElection(leader -> {
                if (electionListener != null) {
                  open = true;
                  future.complete(null);
                  electionListener.close();
                  electionListener = null;
                }
              });
              return future;
            }
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
            closeFuture = context.close().thenRun(() -> {
              closeFuture = null;
              open = false;
            });
          } else {
            closeFuture = openFuture.thenCompose(c -> context.close().thenRun(() -> {
              closeFuture = null;
              open = false;
            }));
          }
        }
      }
    }
    return closeFuture;
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  /**
   * Deletes the Raft server and its logs.
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
    private static final Duration DEFAULT_RAFT_ELECTION_TIMEOUT = Duration.ofMillis(500);
    private static final Duration DEFAULT_RAFT_HEARTBEAT_INTERVAL = Duration.ofMillis(150);
    private static final Duration DEFAULT_RAFT_SESSION_TIMEOUT = Duration.ofMillis(5000);

    private Transport transport;
    private Storage storage;
    private Serializer serializer;
    private StateMachine stateMachine;
    private Address address;
    private Set<Address> cluster;
    private Duration electionTimeout = DEFAULT_RAFT_ELECTION_TIMEOUT;
    private Duration heartbeatInterval = DEFAULT_RAFT_HEARTBEAT_INTERVAL;
    private Duration sessionTimeout = DEFAULT_RAFT_SESSION_TIMEOUT;

    private Builder(Address address, Collection<Address> cluster) {
      this.address = Assert.notNull(address, "address");
      this.cluster = new HashSet<>(Assert.notNull(cluster, "cluster"));
      this.cluster.add(address);
    }

    /**
     * Sets the server transport.
     *
     * @param transport The server transport.
     * @return The server builder.
     * @throws NullPointerException if {@code transport} is null
     */
    public Builder withTransport(Transport transport) {
      this.transport = Assert.notNull(transport, "transport");
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
     * Sets the Raft state machine.
     *
     * @param stateMachine The Raft state machine.
     * @return The Raft builder.
     * @throws NullPointerException if {@code stateMachine} is null
     */
    public Builder withStateMachine(StateMachine stateMachine) {
      this.stateMachine = Assert.notNull(stateMachine, "stateMachine");
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
      if (stateMachine == null)
        throw new ConfigurationException("state machine not configured");

      // If the transport is not configured, attempt to use the default Netty transport.
      if (transport == null) {
        try {
          transport = (Transport) Class.forName("io.atomix.catalyst.transport.NettyTransport").newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
          throw new ConfigurationException("transport not configured");
        }
      }

      // If no serializer instance was provided, create one.
      if (serializer == null) {
        serializer = new Serializer(new PooledDirectAllocator());
      }

      // Resolve serializer serializable types with the ServiceLoaderTypeResolver.
      serializer.resolve(new ServiceLoaderTypeResolver());

      // If the storage is not configured, create a new Storage instance with the configured serializer.
      if (storage == null) {
        storage = Storage.builder()
          .withSerializer(serializer)
          .build();
      }

      ServerContext context = new ServerContext(address, cluster, stateMachine, transport, storage, serializer);
      return new CopycatServer(context, electionTimeout, heartbeatInterval, sessionTimeout);
    }
  }

}
