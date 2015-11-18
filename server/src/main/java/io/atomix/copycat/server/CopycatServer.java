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
import io.atomix.copycat.server.state.ServerContext;
import io.atomix.copycat.server.storage.Storage;

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
 * The lifecycle of the Raft server is managed via the {@link Managed} API methods. To start a server,
 * call {@link Managed#open()} on the server. Once the server has connected to the cluster and found
 * a leader, the returned {@link CompletableFuture} will be completed and the server will be operating.
 * <p>
 * Throughout the lifetime of a server, the server transitions between a variety of
 * {@link CopycatServer.State states}. Call {@link #state()} to get the current state
 * of the server at any given point in time.
 * <p>
 * The {@link #term()} and {@link #leader()} are critical aspects of the Raft consensus algorithm. Initially,
 * when the server is started, the {@link #term()} will be initialized to {@code 0} and {@link #leader()} will
 * be {@code null}. By the time the server is fully started, both {@code term} and {@code leader} will be
 * provided. As the cluster progresses, {@link #term()} will progress monotonically, and for each term,
 * only a single {@link #leader()} will ever be elected.
 * <p>
 * Raft servers are members of a cluster of servers identified by their {@link Address}. Each server
 * must be able to locate other members of the cluster. Throughout the lifetime of a cluster, the membership
 * may change. The {@link #members()} method provides a current view of the cluster from the perspective
 * of a single server. Note that the members list may not be consistent on all nodes at any given time.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface CopycatServer extends Managed<CopycatServer> {

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
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The local server member address.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  static Builder builder(Address clientAddress, Address serverAddress, Address... cluster) {
    return builder(clientAddress, serverAddress, Arrays.asList(cluster));
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
   * @param clientAddress The address through which clients connect to the server.
   * @param serverAddress The local server member address.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  static Builder builder(Address clientAddress, Address serverAddress, Collection<Address> cluster) {
    return new Builder(clientAddress, serverAddress, cluster);
  }

  /**
   * Raft server state types.
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
   */
  long term();

  /**
   * Returns the current Raft leader.
   * <p>
   * If no leader has been elected, the leader address will be {@code null}.
   *
   * @return The current Raft leader or {@code null} if this server does not know of any leader.
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
   */
  Listener<Address> onLeaderElection(Consumer<Address> listener);

  /**
   * Returns a collection of current cluster members.
   * <p>
   * The current members list includes members in all states, including non-voting states. Additionally, because
   * the membership set can change over time, the set of members on one server may not exactly reflect the
   * set of members on another server at any given point in time.
   *
   * @return A collection of current Raft cluster members.
   */
  Collection<Address> members();

  /**
   * Returns the Raft server state.
   * <p>
   * The initial state of a Raft server is {@link State#INACTIVE}. Once the server is {@link #open() started} and
   * until it is explicitly shutdown, the server will be in one of the active states - {@link State#PASSIVE},
   * {@link State#FOLLOWER}, {@link State#CANDIDATE}, or {@link State#LEADER}.
   *
   * @return The Raft server state.
   */
  State state();

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
  Listener<State> onStateChange(Consumer<State> listener);

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
  ThreadContext context();

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
   * Raft server builder.
   */
  class Builder extends io.atomix.catalyst.util.Builder<CopycatServer> {
    private static final Duration DEFAULT_RAFT_ELECTION_TIMEOUT = Duration.ofMillis(1000);
    private static final Duration DEFAULT_RAFT_HEARTBEAT_INTERVAL = Duration.ofMillis(150);
    private static final Duration DEFAULT_RAFT_SESSION_TIMEOUT = Duration.ofMillis(5000);

    private int quorumHint;
    private int backupCount = 1;
    private Transport transport;
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
     * Sets the server quorum hint.
     *
     * @param quorumHint The server quorum hint.
     * @return The server builder.
     * @throws IllegalArgumentException If the quorum hint is not positive.
     */
    public Builder withQuorumHint(int quorumHint) {
      this.quorumHint = Assert.argNot(quorumHint, quorumHint <= 0, "quorum must be positive");
      return this;
    }

    /**
     * Sets the server backup count.
     *
     * @param backupCount The server backup count.
     * @return The server builder.
     * @throws IllegalArgumentException If the backup count is not positive.
     */
    public Builder withBackupCount(int backupCount) {
      this.backupCount = Assert.argNot(backupCount, backupCount <= 0, "backupCount must be positive");
      return this;
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

      // If the quorum hint has not been configured, set the quorum size to the configured number of members.
      if (quorumHint == 0) {
        quorumHint = cluster.size();
      }

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

      ServerContext context = new ServerContext(clientAddress, serverAddress, cluster, quorumHint, backupCount, stateMachine, transport, storage, serializer);
      return new CopycatRaftServer(context, electionTimeout, heartbeatInterval, sessionTimeout);
    }
  }

}
