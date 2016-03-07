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
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.SingleThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.protocol.ClientRequestTypeResolver;
import io.atomix.copycat.protocol.ClientResponseTypeResolver;
import io.atomix.copycat.server.cluster.Cluster;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.state.ConnectionManager;
import io.atomix.copycat.server.state.ServerContext;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.copycat.server.storage.util.StorageSerialization;
import io.atomix.copycat.server.util.ServerSerialization;
import io.atomix.copycat.util.ProtocolSerialization;
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
 * <h2>State machines</h2>
 * Underlying each server is a {@link StateMachine}. The state machine is responsible for maintaining the state with
 * relation to {@link Command}s and {@link Query}s submitted to the server by a client. State machines are provided
 * in a factory to allow servers to transition between stateful and stateless states.
 * <pre>
 *   {@code
 *   Address address = new Address("123.456.789.0", 5000);
 *   Collection<Address> members = Arrays.asList(new Address("123.456.789.1", 5000), new Address("123.456.789.2", 5000));
 *
 *   CopycatServer server = CopycatServer.builder(address, members)
 *     .withStateMachine(MyStateMachine::new)
 *     .build();
 *   }
 * </pre>
 * Server state machines are responsible for registering {@link Command}s which can be submitted to the cluster. Raft
 * relies upon determinism to ensure consistency throughout the cluster, so <em>it is imperative that each server in
 * a cluster have the same state machine with the same commands.</em> State machines are provided to the server as
 * a {@link Supplier factory} to allow servers to {@link Member#promote(Member.Type) transition} between stateful
 * and stateless states.
 * <h2>Transports</h2>
 * By default, the server will use the {@code NettyTransport} for communication. You can configure the transport via
 * {@link CopycatServer.Builder#withTransport(Transport)}. To use the Netty transport, ensure you have the
 * {@code io.atomix.catalyst:catalyst-netty} jar on your classpath.
 * <pre>
 * {@code
 * CopycatServer server = CopycatServer.builder(address, members)
 *   .withStateMachine(MyStateMachine::new)
 *   .withTransport(NettyTransport.builder()
 *     .withThreads(4)
 *     .build())
 *   .build();
 * }
 * </pre>
 * <h2>Storage</h2>
 * As {@link Command}s are received by the server, they're written to the Raft {@link Log} and replicated to other members
 * of the cluster. By default, the log is stored on disk, but users can override the default {@link Storage} configuration
 * via {@link CopycatServer.Builder#withStorage(Storage)}. Most notably, to configure the storage module to store entries in
 * memory instead of disk, configure the {@link StorageLevel}.
 * <pre>
 * {@code
 * CopycatServer server = CopycatServer.builder(address, members)
 *   .withStateMachine(MyStateMachine::new)
 *   .withStorage(Storage.builder()
 *     .withDirectory(new File("logs"))
 *     .withStorageLevel(StorageLevel.DISK)
 *     .build())
 *   .build();
 * }
 * </pre>
 * Servers use the {@code Storage} object to manage the storage of cluster configurations, voting information, and
 * state machine snapshots in addition to logs. See the {@link Storage} documentation for more information.
 * <h2>Serialization</h2>
 * All serialization is performed with a Catalyst {@link Serializer}. The serializer is shared across all components of
 * the server. Users are responsible for ensuring that {@link Command commands} and {@link Query queries} submitted to the
 * cluster can be serialized by the server serializer by registering serializable types as necessary.
 * <p>
 * By default, the server serializer does not allow arbitrary classes to be serialized due to security concerns. However,
 * users can enable arbitrary class serialization by disabling the {@link Serializer#disableWhitelist() whitelisting feature}
 * on the Catalyst {@link Serializer}:
 * <pre>
 *   {@code
 *   server.serializer().disableWhitelist();
 *   }
 * </pre>
 * However, for more efficient serialization, users should explicitly register serializable classes and binary
 * {@link io.atomix.catalyst.serializer.TypeSerializer serializers}. Explicit registration of serializable typs allows
 * types to be serialized using more compact 8- 16- 24- and 32-bit serialization IDs rather than serializing complete
 * class names. Thus, serializable type registration is strongly recommended for production systems.
 * <pre>
 *   {@code
 *   server.serializer().register(MySerializable.class, 123, MySerializableSerializer.class);
 *   }
 * </pre>
 * <h2>Running the server</h2>
 * Once the server has been created, to connect to a cluster simply {@link #start() start} the server. The server API is
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
public class CopycatServer {
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
     * All servers start in this state and return to this state when {@link #stop() stopped}.
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
  private volatile boolean started;

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
   * <p>
   * The server name is provided to the server via the {@link Builder#withName(String) builder configuration}.
   * The name is used internally to manage the server's on-disk state. {@link Log Log},
   * {@link io.atomix.copycat.server.storage.snapshot.SnapshotStore snapshot},
   * and {@link io.atomix.copycat.server.storage.system.MetaStore configuration} files stored on disk use
   * the server name as the prefix.
   *
   * @return The server name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the server storage.
   * <p>
   * The returned {@link Storage} object is the object provided to the server via the {@link Builder#withStorage(Storage) builder}
   * configuration. The storage object is immutable and is intended to provide runtime configuration information only. Users
   * should <em>never open logs, snapshots, or other storage related files</em> through the {@link Storage} API. Doing so
   * can conflict with internal server operations, resulting in the loss of state.
   * <p>
   * To delete the server's on-disk state, use the {@link #delete()} method rather than operating on the {@link Storage}
   * object directly.
   *
   * @return The server storage.
   */
  public Storage storage() {
    return context.getStorage();
  }

  /**
   * Returns the server's cluster configuration.
   * <p>
   * The {@link Cluster} is representative of the server's current view of the cluster configuration. The first time
   * the server is {@link #start() started}, the cluster configuration will be initialized using the {@link Address}
   * list provided to the server {@link #builder(Address, Address...) builder}. For {@link StorageLevel#DISK persistent}
   * servers, subsequent starts will result in the last known cluster configuration being loaded from disk.
   * <p>
   * The returned {@link Cluster} can be used to modify the state of the cluster to which this server belongs. Note,
   * however, that users need not explicitly {@link Cluster#join() join} or {@link Cluster#leave() leave} the cluster
   * since starting and stopping the server results in joining and leaving the cluster respectively.
   *
   * @return The server's cluster configuration.
   */
  public Cluster cluster() {
    return context.getCluster();
  }

  /**
   * Returns the server's binary serializer which is shared among the protocol, state machine, and storage.
   * <p>
   * The returned serializer is linked to all serialization performed within the Copycat server. Serializable types
   * and associated {@link io.atomix.catalyst.serializer.TypeSerializer serializers} that are
   * {@link Serializer#register(Class) registered} on the returned serializer will be reflected in serialization
   * throughout the server, including in the state machine and logs.
   * <p>
   * By default, the returned serializer does not support serialization of arbitrary classes and class names for
   * security reasons. Users must explicitly whitelist serializable types with the serializer to ensure the server
   * can serialize and deserialize required objects, including state machine {@link Command commands} and
   * {@link Query queries} and any objects that need to be serialized within them.
   * <pre>
   *   {@code
   *   server.serializer().register(MySerializableType.class, 123, MySerializableSerializer.class);
   *   }
   * </pre>
   * Alternatively, users can {@link Serializer#disableWhitelist() disable whitelisting} in the Catalyst serializer
   * to ensure that the server can receive operations from any client.
   * <pre>
   *   {@code
   *   server.serializer().disableWhitelist();
   *   }
   * </pre>
   * When whitelisting is disabled, serializable types that are not explicitly {@link Serializer#register(Class) registered}
   * with the serializer will be serialized with their fully qualified class name to be used during deserialization.
   * However, while disabling whitelisting can improve usability, serializing class names can significantly impact the
   * performance of serialization. Therefore, while it's an acceptable solution in development, it is recommended that
   * users explicitly register serializable types for more efficient serialization in production.
   * <p>
   * <em>It's important to note that because the returned {@link Serializer} affects the on-disk binary representation
   * of commands and queries and other objects submitted to the cluster, users must take care to avoid directly changing
   * the binary format of serializable objects once they have been written to the server's logs. Doing so may result in
   * errors during deserialization of objects in the log. To guard against this, serializable types can implement versioning
   * by writing 8-bit version numbers as part of their normal serialization.</em>
   *
   * @return The server serializer.
   */
  public Serializer serializer() {
    return context.getSerializer();
  }

  /**
   * Returns the Copycat server state.
   * <p>
   * The initial state of a Raft server is {@link State#INACTIVE}. Once the server is {@link #start() started} and
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
   * Starts the server asynchronously.
   * <p>
   * When the server is started, the server will attempt to search for an existing cluster by contacting all of
   * the members in the provided members list. If no existing cluster is found, the server will immediately transition
   * to the {@link State#FOLLOWER} state and continue normal Raft protocol operations. If a cluster is found, the server
   * will attempt to join the cluster. Once the server has joined or started a cluster and a leader has been found,
   * the returned {@link CompletableFuture} will be completed.
   *
   * @return A completable future to be completed once the server has joined the cluster and a leader has been found.
   */
  public CompletableFuture<CopycatServer> start() {
    if (started)
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
                  started = true;
                  future.complete(this);
                } else {
                  electionListener = cluster().onLeaderElection(leader -> {
                    if (electionListener != null) {
                      started = true;
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
              started = true;
              future.complete(null);
            });
          } else {
            started = true;
            future.complete(null);
          }
        } else {
          future.completeExceptionally(internalError);
        }
      });
    });

    return future;
  }

  /**
   * Returns a boolean indicating whether the server is running.
   *
   * @return Indicates whether the server is running.
   */
  public boolean isRunning() {
    return started;
  }

  /**
   * Stops the server asynchronously.
   *
   * @return A completable future to be completed once the server has been stopped.
   */
  public CompletableFuture<Void> stop() {
    if (!started)
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
   * Kills the server without leaving the cluster.
   *
   * @return A completable future to be completed once the server has been killed.
   */
  public CompletableFuture<Void> kill() {
    if (!started)
      return Futures.exceptionalFuture(new IllegalStateException("context not open"));

    CompletableFuture<Void> future = new CompletableFuture<>();
    context.getThreadContext().executor().execute(() -> {
      started = false;
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
      started = false;
    });
  }

  /**
   * Deletes the server and its logs.
   * <p>
   * If the server is not already stopped, it will be stopped upon calling this method. Once the server has been
   * shut down, all state persisted on disk by the server will be deleted. On-disk state includes the last known
   * cluster configuration and all logs and snapshots. In the event that the server is restarted after its state
   * has been deleted, the server will start with a new log and no snapshots.
   *
   * @return A completable future to be completed once the server state has been deleted.
   */
  public CompletableFuture<Void> delete() {
    return stop().thenRun(context::delete);
  }

  /**
   * Builds a single-use Copycat server.
   * <p>
   * This builder should be used to programmatically configure and construct a new {@link CopycatServer} instance.
   * The builder provides methods for configuring all aspects of a Copycat server. The {@code CopycatServer.Builder}
   * class cannot be instantiated directly. To create a new builder, use one of the
   * {@link CopycatServer#builder(Address, Address...) server builder factory} methods.
   * <pre>
   *   {@code
   *   CopycatServer.Builder builder = CopycatServer.builder(address, members);
   *   }
   * </pre>
   * Once the server has been configured, use the {@link #build()} method to build the server instance:
   * <pre>
   *   {@code
   *   CopycatServer server = CopycatServer.builder(address, members)
   *     ...
   *     .build();
   *   }
   * </pre>
   * Each server <em>must</em> be configured with a {@link StateMachine}. The state machine is the component of the
   * server that stores state and reacts to commands and queries submitted by clients to the cluster. State machines
   * are provided to the server in the form of a state machine {@link Supplier factory} to allow the server to reconstruct
   * its state when necessary.
   * <pre>
   *   {@code
   *   CopycatServer server = CopycatServer.builder(address, members)
   *     .withStateMachine(MyStateMachine::new)
   *     .build();
   *   }
   * </pre>
   * Similarly critical to the operation of the server are the {@link Transport} and {@link Storage} layers. By default,
   * servers are configured with the {@code io.atomix.catalyst.transport.NettyTransport} transport if it's available on
   * the classpath. Users should provide a {@link Storage} instance to specify how the server stores state changes.
   */
  public static class Builder implements io.atomix.catalyst.util.Builder<CopycatServer> {
    private static final String DEFAULT_NAME = "copycat";
    private static final Duration DEFAULT_ELECTION_TIMEOUT = Duration.ofMillis(750);
    private static final Duration DEFAULT_HEARTBEAT_INTERVAL = Duration.ofMillis(250);
    private static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofMillis(5000);
    private static final Duration DEFAULT_GLOBAL_SUSPEND_TIMEOUT = Duration.ofHours(1);

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
    private Duration globalSuspendTimeout = DEFAULT_GLOBAL_SUSPEND_TIMEOUT;

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
     * @return The server builder.
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
     * Sets the timeout after which suspended global replication will resume and force a partitioned follower
     * to truncate its log once the partition heals.
     *
     * @param globalSuspendTimeout The timeout after which to resume global replication.
     * @return The server builder.
     */
    public Builder withGlobalSuspendTimeout(Duration globalSuspendTimeout) {
      Assert.notNull(globalSuspendTimeout, "globalSuspendTimeout");
      this.globalSuspendTimeout = Assert.argNot(globalSuspendTimeout, globalSuspendTimeout.isNegative() || globalSuspendTimeout.isZero(), "globalSuspendTimeout must be positive");
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
      serializer.resolve(new ProtocolSerialization());
      serializer.resolve(new ServerSerialization());
      serializer.resolve(new StorageSerialization());

      // If the storage is not configured, create a new Storage instance with the configured serializer.
      if (storage == null) {
        storage = new Storage();
      }

      ConnectionManager connections = new ConnectionManager(serverTransport.client());
      ThreadContext threadContext = new SingleThreadContext(String.format("copycat-server-%s-%s", serverAddress, name), serializer);

      ServerContext context = new ServerContext(name, type, serverAddress, clientAddress, cluster, storage, serializer, stateMachineFactory, connections, threadContext);
      context.setElectionTimeout(electionTimeout)
        .setHeartbeatInterval(heartbeatInterval)
        .setSessionTimeout(sessionTimeout)
        .setGlobalSuspendTimeout(globalSuspendTimeout);

      return new CopycatServer(name, clientTransport, serverTransport, context);
    }
  }

}
