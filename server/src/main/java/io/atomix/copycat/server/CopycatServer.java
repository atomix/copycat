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
 * limitations under the License
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
import io.atomix.copycat.server.state.ServerContext;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Provides an interface for managing the lifecycle and state of a Raft server.
 * <p>
 * The lifecycle of the Copycat server is managed via the {@link Managed} API methods. To start a server,
 * call {@link Managed#open()} on the server. Once the server has connected to the cluster and found
 * a leader, the returned {@link CompletableFuture} will be completed and the server will be operating.
 * <p>
 * Servers are created via the {@link CopycatServer#builder(Address, Address...)} API. To create a new server,
 * create a builder, passing the local server {@link Address} and a set of remote server addresses with which
 * to communicate.
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
 * <p>
 * Servers are started by calling {@link #open()} on the {@link CopycatServer} instance.
 * <pre>
 *   {@code
 *   server.open().thenRun(() -> System.out.println("Server started successfully!"));
 *   }
 * </pre>
 * The first time a server is started, it will either start a new cluster or join an existing cluster based on the
 * {@link Address} and membership configuration provided to the server builder. If the server is a member of the
 * provided members list, it will start a new cluster and begin communicating with other members in the members list.
 * If the server is not listed in the members list, it will attempt to join an existing cluster by submitting a configuration
 * change to the provided members. Once the server has been started for the first time, the cluster configuration will
 * be persisted locally via the server's configured {@link Storage} module, and next time the server is started it will
 * join the cluster using the persisted configuration.
 * <p>
 * Once the server is started, it will communicate with the rest of the nodes in the cluster, periodically
 * transitioning between states. A server can transition between a number of states that dictate how it interacts with
 * other nodes in the cluster. Typically, when a server is first started it will either start in the {@link State#FOLLOWER}
 * state (if it's a full voting member) or the {@link State#RESERVE} state (if it's not a voting member). The cluster
 * always attempts to maintain the configured number of active Raft voting members, known as the quorum size. The quorum
 * hint can be configured in the server builder via {@link CopycatServer.Builder#withQuorumHint(int)}. If the number of servers
 * in the cluster is greater than the configured quorum hint, the number of Raft voting members (i.e. {@link State#FOLLOWER}
 * {@link State#CANDIDATE} or {@link State#LEADER}) will always tend towards the quorum hint. Additionally, for each active
 * quorum member, a configurable number of backup servers will be maintained in the {@link State#PASSIVE} state. Passive members
 * are kept up to date with the active voting members, and in the event that an active member becomes unavailable, it will
 * be replaced by a passive member. This backup count can be configured in the builder via {@link CopycatServer.Builder#withBackupCount(int)}.
 * Finally, any servers remaining after the {@code quorumHint} and {@code backupCount} requirements have been met will remain
 * in the {@link State#RESERVE} state. Reserve servers do not persist state changes. In the event that an active or passive
 * server becomes unavailable, a reserve server will be promoted to take its place.
 * }
 * <p>
 * Users can listen for state transitions via {@link #onStateChange(Consumer)}:
 * <pre>
 * {@code
 * server.onStateChange(state -> {
 *   if (state == CopycatServer.State.LEADER) {
 *     System.out.println("Server elected leader!");
 *   }
 * });
 * }
 * <p>
 * <b>State machines</b>
 * <p>
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
 *
 * @see StateMachine
 * @see Transport
 * @see Storage
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface CopycatServer extends Managed<CopycatServer> {

  /**
   * Returns a new Copycat server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Copycat cluster. The local server {@link Address} does
   * not have to be present in the address list. If the address is not listed in the provided {@code members} list,
   * the server will attempt to join the provided members when the server is started. Once the server has been started
   * and successfully joined the cluster, thereafter it will use the most recently known configuration at startup.
   * <p>
   * The returned server builder will use the {@code NettyTransport} by default. Additionally, serializable types will
   * be registered using the {@link ServiceLoaderTypeResolver}. To register serializable types for the server, simply
   * add a {@link io.atomix.catalyst.serializer.Serializer} or {@link io.atomix.catalyst.serializer.CatalystSerializable}
   * file to your {@code META-INF/services} folder on the classpath.
   *
   * @param address The address through which all communication takes place.
   * @param members The cluster members to which to connect.
   * @return The server builder.
   */
  static Builder builder(Address address, Address... members) {
    return builder(address, address, Arrays.asList(members));
  }

  /**
   * Returns a new Copycat server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Copycat cluster. The local server {@link Address} does
   * not have to be present in the address list. If the address is not listed in the provided {@code members} list,
   * the server will attempt to join the provided members when the server is started. Once the server has been started
   * and successfully joined the cluster, thereafter it will use the most recently known configuration at startup.
   * <p>
   * The returned server builder will use the {@code NettyTransport} by default. Additionally, serializable types will
   * be registered using the {@link ServiceLoaderTypeResolver}. To register serializable types for the server, simply
   * add a {@link io.atomix.catalyst.serializer.Serializer} or {@link io.atomix.catalyst.serializer.CatalystSerializable}
   * file to your {@code META-INF/services} folder on the classpath.
   *
   * @param address The address through which all communication takes place.
   * @param members The cluster members to which to connect.
   * @return The server builder.
   */
  static Builder builder(Address address, Collection<Address> members) {
    return new Builder(address, address, members);
  }

  /**
   * Returns a new Copycat server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Copycat cluster. The local server {@link Address} does
   * not have to be present in the address list. If the address is not listed in the provided {@code members} list,
   * the server will attempt to join the provided members when the server is started. Once the server has been started
   * and successfully joined the cluster, thereafter it will use the most recently known configuration at startup.
   * <p>
   * Clients will communicate with the server via the provided {@code clientAddress}, and servers will communicate via
   * the {@code serverAddress}. When listing member addresses for other servers in the cluster, the {@code members} list
   * should contain <em>server</em> addresses. Alternatively, when listing server addresses to which to connect on a client,
   * the client should list <em>client</em> addresses.
   * <p>
   * The returned server builder will use the {@code NettyTransport} by default. Additionally, serializable types will
   * be registered using the {@link ServiceLoaderTypeResolver}. To register serializable types for the server, simply
   * add a {@link io.atomix.catalyst.serializer.Serializer} or {@link io.atomix.catalyst.serializer.CatalystSerializable}
   * file to your {@code META-INF/services} folder on the classpath.
   *
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The local server member address.
   * @param members The cluster members to which to connect.
   * @return The server builder.
   */
  static Builder builder(Address clientAddress, Address serverAddress, Address... members) {
    return builder(clientAddress, serverAddress, Arrays.asList(members));
  }

  /**
   * Returns a new Copycat server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Copycat cluster. The local server {@link Address} does
   * not have to be present in the address list. If the address is not listed in the provided {@code members} list,
   * the server will attempt to join the provided members when the server is started. Once the server has been started
   * and successfully joined the cluster, thereafter it will use the most recently known configuration at startup.
   * <p>
   * Clients will communicate with the server via the provided {@code clientAddress}, and servers will communicate via
   * the {@code serverAddress}. When listing member addresses for other servers in the cluster, the {@code members} list
   * should contain <em>server</em> addresses. Alternatively, when listing server addresses to which to connect on a client,
   * the client should list <em>client</em> addresses.
   * <p>
   * The returned server builder will use the {@code NettyTransport} by default. Additionally, serializable types will
   * be registered using the {@link ServiceLoaderTypeResolver}. To register serializable types for the server, simply
   * add a {@link io.atomix.catalyst.serializer.Serializer} or {@link io.atomix.catalyst.serializer.CatalystSerializable}
   * file to your {@code META-INF/services} folder on the classpath.
   *
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The local server member address.
   * @param members The cluster members to which to connect.
   * @return The server builder.
   */
  static Builder builder(Address clientAddress, Address serverAddress, Collection<Address> members) {
    return new Builder(clientAddress, serverAddress, members);
  }

  /**
   * Copycat server state types.
   * <p>
   * States represent the context of the server's internal state machine. Throughout the lifetime of a server,
   * the server will periodically transition between states based on requests, responses, and timeouts.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  enum State {

    /**
     * Represents the state of an inactive server.
     * <p>
     * All servers start in this state and return to this state when {@link #close() stopped}.
     */
    INACTIVE,

    /**
     * Represents the state of a server that does not store state.
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
     * exist at any given time, but Raft guarantees that no two leaders will exist for the same {@link #term()}.
     */
    LEADER

  }

  /**
   * Returns the current Raft term.
   * <p>
   * The term is a monotonically increasing number that essentially acts as a logical time for the cluster. For any
   * given term, Raft guarantees that only one {@link #leader()} can be elected, but note that a leader may also
   * not yet exist for the term.
   *
   * @return The current Raft term.
   * @throws IllegalStateException If the server is not open
   */
  long term();

  /**
   * Returns the current Raft leader.
   * <p>
   * If no leader has been elected, the leader address will be {@code null}.
   *
   * @return The current Raft leader or {@code null} if this server does not know of any leader.
   * @throws IllegalStateException If the server is not open
   */
  Address leader();

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
   * @throws IllegalStateException If the server is not open
   */
  Listener<Address> onLeaderElection(Consumer<Address> listener);

  /**
   * Returns a collection of current cluster members.
   * <p>
   * The current members list includes members in all states, including non-voting states. Additionally, because
   * the membership set can change over time, the set of members on one server may not exactly reflect the
   * set of members on another server at any given point in time.
   *
   * @return A collection of current Copycat cluster members.
   * @throws IllegalStateException If the server is not open
   */
  Collection<Address> members();

  /**
   * Returns the Copycat server state.
   * <p>
   * The initial state of a Copycat server is {@link State#INACTIVE}. Once the server is {@link #open() started} and
   * until it is explicitly shutdown, the server will be in one of the active states - {@link State#RESERVE},
   * {@link State#PASSIVE}, {@link State#FOLLOWER}, {@link State#CANDIDATE}, or {@link State#LEADER}.
   *
   * @return The Copycat server state.
   * @throws IllegalStateException If the server is not open
   */
  State state();

  /**
   * Returns a boolean value indicating whether this server is currently a reserve member of the cluster.
   * <p>
   * Reserve members do not receive state changes but maintain the current configuration of the cluster.
   * In the event that a {@link State#PASSIVE} member is promoted to an active Raft voting member, the passive
   * member will be replaced by a {@link State#RESERVE} server.
   *
   * @return Indicates whether this server is a reserve member of the cluster.
   */
  default boolean isReserve() {
    return isOpen() && state() == State.RESERVE;
  }

  /**
   * Returns a boolean value indicating whether this server is currently a passive member of the cluster.
   * <p>
   * Passive members do not participate in the Raft consensus algorithm, but they do receive state changes.
   * Followers in the Raft cluster will always attempt to keep passive servers as up to date as possible.
   * In the event that a Raft voting member fails or otherwise becomes unavailable, a passive server will
   * be promoted to take its place.
   *
   * @return Indicates whether this server is a passive member of the cluster.
   */
  default boolean isPassive() {
    return isOpen() && state() == State.PASSIVE;
  }

  /**
   * Returns a boolean value indicating whether this server is currently an active member of the cluster.
   * <p>
   * Active members participate fully in the Raft consensus algorithm and are always in one of the Raft
   * states, i.e. {@link State#FOLLOWER}, {@link State#CANDIDATE}, or {@link State#LEADER}.
   *
   * @return Indicates whether this server is an active member of the cluster.
   */
  default boolean isActive() {
    if (!isOpen())
      return false;
    State state = state();
    return state == State.FOLLOWER || state == State.CANDIDATE || state == State.LEADER;
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
   * @throws IllegalStateException If the server is not open
   */
  Listener<State> onStateChange(Consumer<State> listener);

  /**
   * Returns the server execution context.
   * <p>
   * The thread context is the event loop that this server uses to communicate other Copycat servers.
   * Implementations must guarantee that all asynchronous {@link java.util.concurrent.CompletableFuture} callbacks are
   * executed on a single thread via the returned {@link io.atomix.catalyst.util.concurrent.ThreadContext}.
   * <p>
   * The {@link io.atomix.catalyst.util.concurrent.ThreadContext} can also be used to access the Copycat server's internal
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
   * @throws IllegalStateException If the server is not open
   */
  ThreadContext context();

  /**
   * Starts the Copycat server asynchronously.
   * <p>
   * When the server is started, if the server is a member of the current configuration, it will start in the
   * appropriate state. If the server is being started for the first time and is not listed in the members list,
   * the server will attempt to join the cluster. Once the server has joined the cluster, the returned
   * {@link CompletableFuture} will be completed.
   *
   * @return A completable future to be completed once the server has joined the cluster and a leader has been found.
   */
  @Override
  CompletableFuture<CopycatServer> open();

  /**
   * Returns a boolean value indicating whether the server is running.
   * <p>
   * Once {@link #open()} is called and the returned {@link CompletableFuture} is completed (meaning this server found
   * a cluster leader), this method will return {@code true} until {@link #close() closed}.
   *
   * @return Indicates whether the server is running.
   */
  @Override
  boolean isOpen();

  /**
   * Deletes the Copycat server and its logs.
   *
   * @return A completable future to be completed once the server has been deleted.
   */
  CompletableFuture<Void> delete();

  /**
   * Copycat server builder.
   */
  class Builder extends io.atomix.catalyst.util.Builder<CopycatServer> {
    private static final Duration DEFAULT_RAFT_ELECTION_TIMEOUT = Duration.ofMillis(1000);
    private static final Duration DEFAULT_RAFT_HEARTBEAT_INTERVAL = Duration.ofMillis(150);
    private static final Duration DEFAULT_RAFT_SESSION_TIMEOUT = Duration.ofMillis(5000);

    private int quorumHint;
    private int backupCount = 1;
    private Transport clientTransport;
    private Transport serverTransport;
    private Storage storage;
    private Serializer serializer;
    private StateMachine stateMachine;
    private Address clientAddress;
    private Address serverAddress;
    private Set<Address> cluster;
    private Duration electionTimeout = DEFAULT_RAFT_ELECTION_TIMEOUT;
    private Duration heartbeatInterval = DEFAULT_RAFT_HEARTBEAT_INTERVAL;
    private Duration sessionTimeout = DEFAULT_RAFT_SESSION_TIMEOUT;

    private Builder(Address clientAddress, Address serverAddress, Collection<Address> cluster) {
      this.clientAddress = Assert.notNull(clientAddress, "clientAddress");
      this.serverAddress = Assert.notNull(serverAddress, "serverAddress");
      this.cluster = new HashSet<>(Assert.notNull(cluster, "cluster"));
    }

    /**
     * Sets the server quorum hint, returning the server builder for method chaining.
     * <p>
     * The quorum hint is the number of servers that should participate in the Raft consensus algorithm
     * as full voting members. If the number of servers in the cluster is less than the quorum hint, all
     * servers will be voting members, otherwise the system will attempt to promote and demote servers as
     * necessary to maintain the configured quorum size.
     * <p>
     * The quorum hint should always be an odd number for the greatest fault tolerance. Increasing the quorum
     * hint will result in higher latency for operations committed to the cluster. Decreasing the quorum hint
     * will result in lower tolerance for failures but also lower latency for writes.
     *
     * @param quorumHint The number of servers to participate in the Raft consensus algorithm.
     * @return The server builder.
     * @throws IllegalArgumentException If the quorum hint is not positive.
     */
    public Builder withQuorumHint(int quorumHint) {
      this.quorumHint = Assert.argNot(quorumHint, quorumHint <= 0, "quorum must be positive");
      return this;
    }

    /**
     * Sets the server backup count, returning the server builder for method chaining.
     * <p>
     * The backup count is the <em>maximum</em> number of backup servers per active voting member of the
     * Copycat cluster. Backup servers are kept up to date by Raft followers and will be promoted to active
     * Raft voting members in the event of a failure of a voting member. Increasing the number of backup
     * servers will increase the load on the cluster, but note that it should not increase the latency of
     * updates since backups do not participate in commitment of operations to the cluster. Decreasing
     * the number of backup servers may increase the amount of time necessary to replace a failed server
     * and regain increased availability.
     *
     * @param backupCount The number of backup servers per active server.
     * @return The server builder.
     * @throws IllegalArgumentException If the backup count is not positive.
     */
    public Builder withBackupCount(int backupCount) {
      this.backupCount = Assert.argNot(backupCount, backupCount <= 0, "backupCount must be positive");
      return this;
    }

    /**
     * Sets the client and server transport, returning the server builder for method chaining.
     * <p>
     * The configured transport should be the same transport as all other nodes in the cluster.
     * Additionally, if no client transport is explicitly provided, the configured transport will
     * be used for client communication. If no transport is explicitly provided, the server will
     * default to the {@code NettyTransport} if available on the classpath.
     *
     * @param transport The server transport.
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
     * Sets the client transport, returning the server builder for method chaining.
     * <p>
     * The configured transport should be used by all clients when connecting to the cluster. If no
     * client transport is explicitly configured, the server transport will be used or the transport will
     * default to the {@code NettyTransport} if available on the classpath.
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
     * Sets the server transport, returning the server builder for method chaining.
     * <p>
     * The configured transport should be the same transport as all other nodes in the cluster.
     * Additionally, if no client transport is explicitly provided, the configured transport will
     * be used for client communication. If no transport is explicitly provided, the server will
     * default to the {@code NettyTransport} if available on the classpath.
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
     * Sets the Copycat serializer, returning the server builder for method chaining.
     * <p>
     * The serializer will be used to serialize and deserialize operations that are sent over the wire.
     * Internal server classes will automatically be registered with the configured serializer. Additional
     * classes can be either registered on the serializer or via the {@link java.util.ServiceLoader} pattern.
     *
     * @param serializer The Copycat serializer.
     * @return The Copycat server builder.
     * @throws NullPointerException if {@code serializer} is null
     */
    public Builder withSerializer(Serializer serializer) {
      this.serializer = Assert.notNull(serializer, "serializer");
      return this;
    }

    /**
     * Sets the storage module, returning the server builder for method chaining.
     * <p>
     * The storage module is the interface the server will use to store the persistent replicated log.
     * For simple configurations, users can simply construct a {@link Storage} object:
     * <pre>
     *   {@code
     *   CopycatServer server = CopycatServer.builder(address, members)
     *     .withStorage(new Storage("logs"))
     *     .build();
     *   }
     * </pre>
     * For more complex storage configurations, use the {@link io.atomix.copycat.server.storage.Storage.Builder}:
     * <pre>
     *   {@code
     *   CopycatServer server = CopycatServer.builder(address, members)
     *     .withStorage(Storage.builder()
     *       .withDirectory("logs")
     *       .withStorageLevel(StorageLevel.MAPPED)
     *       .withCompactionThreads(2)
     *       .build())
     *     .build();
     *   }
     * </pre>
     *
     * @param storage The storage module.
     * @return The Copycat server builder.
     * @throws NullPointerException if {@code storage} is null
     */
    public Builder withStorage(Storage storage) {
      this.storage = Assert.notNull(storage, "storage");
      return this;
    }

    /**
     * Sets the Copycat state machine, returning the server builder for method chaining.
     * <p>
     * The state machine is the component that manages state within the server. When clients submit
     * {@link io.atomix.copycat.client.Command commands} and {@link io.atomix.copycat.client.Query queries}
     * to the cluster, those operations are logged and replicated and ultimately applied to the state machine
     * on each server. All servers in the cluster must be configured with the same state machine, and all
     * state machines must behave deterministically to uphold Copycat's consistency guarantees.
     *
     * @param stateMachine The Copycat state machine.
     * @return The Copycat server builder.
     * @throws NullPointerException if {@code stateMachine} is null
     */
    public Builder withStateMachine(StateMachine stateMachine) {
      this.stateMachine = Assert.notNull(stateMachine, "stateMachine");
      return this;
    }

    /**
     * Sets the Raft election timeout, returning the server builder for method chaining.
     * <p>
     * The election timeout is the duration since last contact with the cluster leader after which
     * the server should start a new election. The election timeout should always be significantly
     * larger than {@link #withHeartbeatInterval(Duration)} in order to prevent unnecessary elections.
     *
     * @param electionTimeout The Raft election timeout duration.
     * @return The Copycat server builder.
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
     * Sets the Raft heartbeat interval, returning the server builder for method chaining.
     * <p>
     * The heartbeat interval is the interval at which the server, if elected leader, should contact
     * other servers within the cluster to maintain its leadership. The heartbeat interval should
     * always be some fraction of {@link #withElectionTimeout(Duration)}.
     *
     * @param heartbeatInterval The Raft heartbeat interval duration.
     * @return The Copycat server builder.
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
     * Sets the Raft session timeout, returning the server builder for method chaining.
     * <p>
     * The session timeout is assigned by the server to a client which opens a new session. The session timeout
     * dictates the interval at which the client must send keep-alive requests to the cluster to maintain its
     * session. If a client fails to communicate with the cluster for larger than the configured session
     * timeout, its session may be expired.
     * <p>
     * Note that if multiple servers in the cluster use different session timeouts, the session timeout for
     * each client's session may differ based on the server through which they registered their session. It's
     * recommended that servers configure the same session timeout for consistency and predictability.
     *
     * @param sessionTimeout The Raft session timeout duration.
     * @return The Copycat server builder.
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
     * Builds the Copycat server.
     * <p>
     * If no {@link Transport} was configured for the server, the builder will attempt to create a
     * {@code NettyTransport} instance. If {@code io.atomix.catalyst.transport.NettyTransport} is not available
     * on the classpath, a {@link ConfigurationException} will be thrown.
     * <p>
     * Once the server is built, it is not yet connected to the cluster. To connect the server to the cluster,
     * call the asynchronous {@link #open()} method.
     *
     * @throws ConfigurationException if a state machine is not configured
     */
    @Override
    public CopycatServer build() {
      if (stateMachine == null)
        throw new ConfigurationException("state machine not configured");

      // If the quorum hint has not been configured, set the quorum size to the configured number of members.
      if (quorumHint == 0) {
        quorumHint = cluster.size();
      }

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

      ServerContext context = new ServerContext(clientAddress, clientTransport, serverAddress, serverTransport, cluster, quorumHint, backupCount, stateMachine, storage, serializer);
      return new CopycatRaftServer(context, electionTimeout, heartbeatInterval, sessionTimeout);
    }
  }

}
