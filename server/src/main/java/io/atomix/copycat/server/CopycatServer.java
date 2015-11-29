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
import io.atomix.catalyst.transport.Server;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Managed;
import io.atomix.catalyst.util.concurrent.SingleThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.server.cluster.ConnectionManager;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.cluster.RaftMemberType;
import io.atomix.copycat.server.state.ServerContext;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.MetaStore;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

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
 *
 * @see StateMachine
 * @see Transport
 * @see Storage
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatServer implements Managed<CopycatServer> {

  /**
   * Returns a new Copycat server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Copycat cluster. The local server {@link Address} does
   * not have to be present in the address list.
   * <p>
   * The returned server builder will use the {@code NettyTransport} by default. Additionally, serializable types will
   * be registered using the {@link ServiceLoaderTypeResolver}. To register serializable types for the server, simply
   * add a {@link io.atomix.catalyst.serializer.Serializer} or {@link io.atomix.catalyst.serializer.CatalystSerializable}
   * file to your {@code META-INF/services} folder on the classpath.
   *
   * @param address The address through which clients and servers connect to this server.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  public static Builder builder(Address address, Address... cluster) {
    return builder(address, address, Arrays.asList(cluster));
  }

  /**
   * Returns a new Copycat server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Copycat cluster. The local server {@link Address} does
   * not have to be present in the address list.
   * <p>
   * The returned server builder will use the {@code NettyTransport} by default. Additionally, serializable types will
   * be registered using the {@link ServiceLoaderTypeResolver}. To register serializable types for the server, simply
   * add a {@link io.atomix.catalyst.serializer.Serializer} or {@link io.atomix.catalyst.serializer.CatalystSerializable}
   * file to your {@code META-INF/services} folder on the classpath.
   *
   * @param address The address through which clients and servers connect to this server.
   * @param cluster The cluster members to which to connect.
   * @return The server builder.
   */
  public static Builder builder(Address address, Collection<Address> cluster) {
    return new Builder(address, address, cluster);
  }

  /**
   * Returns a new Copycat server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Copycat cluster. The local server {@link Address} does
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
  public static Builder builder(Address clientAddress, Address serverAddress, Address... cluster) {
    return builder(clientAddress, serverAddress, Arrays.asList(cluster));
  }

  /**
   * Returns a new Copycat server builder.
   * <p>
   * The provided {@link Address} is the address to which to bind the server being constructed. The provided set of
   * members will be used to connect to the other members in the Copycat cluster. The local server {@link Address} does
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
  public static Builder builder(Address clientAddress, Address serverAddress, Collection<Address> cluster) {
    return new Builder(clientAddress, serverAddress, cluster);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(CopycatServer.class);
  private final Address clientAddress;
  private final Address serverAddress;
  private final Collection<Address> members;
  private final StateMachine stateMachine;
  private final Transport clientTransport;
  private final Transport serverTransport;
  private final Storage storage;
  private final Duration electionTimeout;
  private final Duration heartbeatInterval;
  private final Duration sessionTimeout;
  private final ThreadContext context;
  private Server clientServer;
  private Server internalServer;
  private ServerContext state;
  private CompletableFuture<CopycatServer> openFuture;
  private CompletableFuture<Void> closeFuture;
  private volatile boolean open;

  protected CopycatServer(Address clientAddress, Transport clientTransport, Address serverAddress, Transport serverTransport, Collection<Address> members, StateMachine stateMachine, Storage storage, Serializer serializer, Duration electionTimeout, Duration heartbeatInterval, Duration sessionTimeout) {
    this.clientAddress = Assert.notNull(clientAddress, "clientAddress");
    this.serverAddress = Assert.notNull(serverAddress, "serverAddress");
    this.clientTransport = Assert.notNull(clientTransport, "clientTransport");
    this.serverTransport = Assert.notNull(serverTransport, "serverTransport");
    this.members = Assert.notNull(members, "members");
    this.stateMachine = Assert.notNull(stateMachine, "stateMachine");
    this.storage = Assert.notNull(storage, "storage");
    this.context = new SingleThreadContext("copycat-server-" + serverAddress, serializer);
    this.electionTimeout = Assert.notNull(electionTimeout, "electionTimeout");
    this.heartbeatInterval = Assert.notNull(heartbeatInterval, "heartbeatInterval");
    this.sessionTimeout = Assert.notNull(sessionTimeout, "sessionTimeout");

    storage.serializer().resolve(new ServiceLoaderTypeResolver());
    serializer.resolve(new ServiceLoaderTypeResolver());
  }

  /**
   * Server state.
   */
  public interface State {
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
    Assert.state(isOpen(), "server not open");
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
    Assert.state(isOpen(), "server not open");
    Member leader = state.getLeader();
    return leader != null ? leader.serverAddress() : null;
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
    Assert.state(isOpen(), "server not open");
    return state.onLeaderElection(listener);
  }

  /**
   * Returns the Copycat server state.
   *
   * @return The Raft server state.
   */
  public State state() {
    Assert.state(isOpen(), "server not open");
    return state.getState();
  }

  /**
   * Registers a state change listener.
   * <p>
   * Throughout the lifetime of the cluster, the server will periodically transition between various {@link CopycatServer.State states}.
   * Users can listen for and react to state change events. To determine when this server is elected leader, simply
   * listen for the leader state.
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
    Assert.state(isOpen(), "server not open");
    return state.onStateChange(listener);
  }

  /**
   * Returns a collection of current cluster members.
   * <p>
   * The current members list includes members in all states, including non-voting states. Additionally, because
   * the membership set can change over time, the set of members on one server may not exactly reflect the
   * set of members on another server at any given point in time.
   *
   * @return A collection of current Copycat cluster members.
   */
  public Collection<Address> members() {
    Assert.state(isOpen(), "server not open");
    return state.getMembers();
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
    Assert.state(isOpen(), "server not open");
    return state.getThreadContext();
  }

  /**
   * Starts the Copycat server asynchronously.
   * <p>
   * When the server is started, the server will attempt to search for an existing cluster by contacting all of
   * the members in the provided members list. If no existing cluster is found, the server will immediately transition
   * to the follower state and continue normal Raft protocol operations. If a cluster is found, the server
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
          if (closeFuture == null) {
            openFuture = completeOpen();
          } else {
            openFuture = closeFuture.thenCompose(v -> completeOpen());
          }
        }
      }
    }
    return openFuture;
  }

  /**
   * Starts the server.
   */
  private CompletableFuture<CopycatServer> completeOpen() {
    CompletableFuture<CopycatServer> future = new CompletableFuture<>();

    context.executor().execute(() -> {

      // Open the meta store.
      MetaStore meta = storage.openMetaStore("copycat");

      // Open the log.
      Log log = storage.openLog("copycat");

      // Setup the server and connection manager.
      internalServer = serverTransport.server();
      ConnectionManager connections = new ConnectionManager(serverTransport.client());

      internalServer.listen(serverAddress, c -> state.connectServer(c)).whenComplete((internalResult, internalError) -> {
        if (internalError == null) {
          state = new ServerContext(new Member(RaftMemberType.INACTIVE, serverAddress, clientAddress), members, meta, log, stateMachine, connections, context)
            .setElectionTimeout(electionTimeout)
            .setHeartbeatInterval(heartbeatInterval)
            .setSessionTimeout(sessionTimeout);

          // If the client address is different than the server address, start a separate client server.
          if (!clientAddress.equals(serverAddress)) {
            clientServer = clientTransport.server();
            clientServer.listen(clientAddress, c -> state.connectClient(c)).whenComplete((clientResult, clientError) -> {
              open = true;
              future.complete(this);
            });
          } else {
            open = true;
            future.complete(this);
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

  /**
   * Returns a boolean value indicating whether the server is running.
   * <p>
   * Once {@link #open()} is called and the returned {@link CompletableFuture} is completed (meaning this server found
   * a cluster leader), this method will return {@code true} until {@link #close() closed}.
   *
   * @return Indicates whether the server is running.
   */
  @Override
  public CompletableFuture<Void> close() {
    if (!open)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (closeFuture == null) {
          if (openFuture == null) {
            closeFuture = completeClose();
          } else {
            closeFuture = openFuture.thenCompose(v -> completeClose());
          }
        }
      }
    }
    return closeFuture;
  }

  /**
   * Stops the server.
   */
  private CompletableFuture<Void> completeClose() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    state.leave().whenComplete((leaveResult, leaveError) -> {
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

      // Close and reset the server state.
      state.close();
      state = null;
    });

    return future.whenComplete((result, error) -> {
      state = null;
      context.close();
      open = false;
    });
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
    return close().thenRun(() -> {
      // Delete the metadata store.
      MetaStore meta = storage.openMetaStore("copycat");
      meta.close();
      meta.delete();

      // Delete the log.
      Log log = storage.openLog("copycat");
      log.close();
      log.delete();
    });
  }

  /**
   * Raft server builder.
   */
  public static class Builder extends io.atomix.catalyst.util.Builder<CopycatServer> {
    private static final Duration DEFAULT_RAFT_ELECTION_TIMEOUT = Duration.ofMillis(1000);
    private static final Duration DEFAULT_RAFT_HEARTBEAT_INTERVAL = Duration.ofMillis(150);
    private static final Duration DEFAULT_RAFT_SESSION_TIMEOUT = Duration.ofMillis(5000);

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

      return new CopycatServer(clientAddress, clientTransport, serverAddress, serverTransport, cluster, stateMachine, storage, serializer, electionTimeout, heartbeatInterval, sessionTimeout);
    }
  }

}
