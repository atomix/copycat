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
package io.atomix.copycat.client;

import io.atomix.catalyst.concurrent.CatalystThreadFactory;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.impl.DefaultCopycatClient;
import io.atomix.copycat.client.session.CopycatSession;
import io.atomix.copycat.protocol.ClientRequestTypeResolver;
import io.atomix.copycat.protocol.ClientResponseTypeResolver;
import io.atomix.copycat.session.Session;
import io.atomix.copycat.util.ProtocolSerialization;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

/**
 * Provides an interface for submitting {@link Command commands} and {@link Query} queries to the Copycat cluster.
 * <p>
 * Copycat clients are responsible for connecting to the cluster and submitting {@link Command commands} and {@link Query queries}
 * that operate on the cluster's replicated state machine. Copycat clients interact with one or more nodes in a Copycat cluster
 * through a session. When the client is {@link #connect() connected}, the client will attempt to communicate with one of the known member
 * {@link Address} provided to the builder. As long as the client can communicate with at least one correct member of the
 * cluster, it can register a session. Once the client is able to register a {@link Session}, it will receive an updated list
 * of members for the entire cluster and thereafter be allowed to communicate with all servers.
 * <p>
 * Sessions are created by registering the client through the cluster leader. Clients always connect to a single node in the
 * cluster, and in the event of a node failure or partition, the client will detect the failure and reconnect to a correct server.
 * <p>
 * Clients periodically send <em>keep-alive</em> requests to the server to which they're connected. The keep-alive request
 * interval is determined by the cluster's session timeout, and the session timeout is determined by the leader's configuration
 * at the time that the session is registered. This ensures that clients cannot be misconfigured with a keep-alive interval
 * greater than the cluster's session timeout.
 * <p>
 * Clients communicate with the distributed state machine by submitting {@link Command commands} and {@link Query queries} to
 * the cluster through the {@link CopycatSession#submit(Command)} and {@link CopycatSession#submit(Query)} methods respectively:
 * <pre>
 *   {@code
 *   client.submit(new PutCommand("foo", "Hello world!")).thenAccept(result -> {
 *     System.out.println("Result is " + result);
 *   });
 *   }
 * </pre>
 * All client methods are fully asynchronous and return {@link CompletableFuture}. To block until a method is complete, use
 * the {@link CompletableFuture#get()} or {@link CompletableFuture#join()} methods.
 * <p>
 * <h3>Sessions</h3>
 * <p>
 * Sessions work to provide linearizable semantics for client {@link Command commands}. When a command is submitted to the cluster,
 * the command will be forwarded to the leader where it will be logged and replicated. Once the command is stored on a majority
 * of servers, the leader will apply it to its state machine and respond.
 * <p>
 * Sessions also allow {@link Query queries} (read-only requests) submitted by the client to optionally be executed on follower
 * nodes. When a query is submitted to the cluster, the query's {@link Query#consistency()} will be used to determine how the
 * query is handled. For queries with stronger consistency levels, they will be forwarded to the cluster's leader. For weaker
 * consistency queries, they may be executed on follower nodes according to the consistency level constraints. See the
 * {@link Query.ConsistencyLevel} documentation for more info.
 * <p>
 * Throughout the lifetime of a client, the client may operate on the cluster via multiple sessions according to the configured
 * recovery strategy. In the event that the client's session expires, the client may register a new session and continue
 * to submit operations under the recovered session. The client will always attempt to ensure commands submitted are eventually
 * committed to the cluster even across sessions. If a command is submitted under one session but is not completed before the
 * session is lost and a new session is established, the client will resubmit pending commands from the prior session under
 * the new session. This, though, may break linearizability guarantees since linearizability is only guaranteed within the context
 * if a session.
 * <p>
 * Users should watch the client for {@link io.atomix.copycat.client.CopycatClient.State state} changes to determine when
 * linearizability guarantees may be broken.
 * <p>
 * <pre>
 *   {@code
 *   client.onStateChange(state -> {
 *     if (state == CopycatClient.State.SUSPENDED) {
 *
 *     }
 *   });
 *   }
 * </pre>
 * The most notable client state is the {@link CopycatClient.State#SUSPENDED SUSPENDED} state. This state indicates that
 * the client cannot communicate with the cluster and consistency guarantees <em>may</em> have been broken. While in this
 * state, the client's session from the perspective of servers may timeout, the {@link Session} events sent to the client
 * by the cluster may be lost.
 * <h3>Session events</h3>
 * Clients can receive arbitrary event notifications from the cluster by registering an event listener via
 * {@link CopycatSession#onEvent(String, Consumer)}. When a command is applied to a state machine, the state machine may publish any number
 * of events to any open session. Events will be sent to the client by the server to which the client is connected as dictated
 * by the configured {@link CommunicationStrategy}. In the event a client is disconnected from a server, events will be
 * retained in memory on all servers until the client reconnects to another server or its session expires. Once a client
 * reconnects to a new server, the new server will resume sending session events to the client.
 * <pre>
 *   {@code
 *   client.<ChangeEvent>onEvent("change", change -> {
 *     System.out.println("value changed from " + change.oldValue() + " to " + change.newValue());
 *   });
 *   }
 * </pre>
 * <h3>Session consistency</h3>
 * Sessions guarantee linearizability for all <em>commands</em> submitted by the client within the context of a session.
 * This means when the client submits a command, the command is guaranteed to be applied to the replicated state machine
 * between invocation and response. Furthermore, concurrent operations from a single client are guaranteed to be applied
 * in FIFO order even when switching between writes to the leader and reads from followers. In the event that a session
 * is expired by the cluster, linearizability guarantees are lost. It's possible for a command to be applied to the cluster
 * without a successful response if the client's session expires.
 * <p>
 * Clients guarantee that all operations submitted to the cluster will be applied in sequential order and responses will
 * be received by the client in sequential order. This means the {@link CompletableFuture}s returned when submitting
 * operations through the client are guaranteed to be completed in the order in which they were created.
 * <p>
 * Sequential consistency is also guaranteed for {@link CopycatSession#onEvent(String, Consumer) events} received by a client, and events
 * are sequenced with command and query responses. If a client submits a command that publishes an event and then immediately
 * submits a concurrent query, the client will first receive the command response, then the event message, then the query
 * response.
 * <p>
 * All events and responses are received in the client's event thread. Because all operation results are received on the same
 * thread, it is critical that clients not block the event thread. If clients need to perform blocking actions on response to
 * an event or response, do so on another thread.
 * <h3>Serialization</h3>
 * All {@link Command commands}, {@link Query queries}, and session {@link CopycatSession#onEvent(String, Consumer) events} must be
 * serializable by the {@link Serializer} associated with the client. Serializable types can be registered at any time.
 * To register a serializable type and serializer, use the {@link Serializer#register(Class) register} methods.
 * <pre>
 *   {@code
 *   client.serializer().register(SetCommand.class, new SetCommandSerializer());
 *   }
 * </pre>
 * By default, all primitives and many collections are supported. Catalyst also provides support for common serialization
 * frameworks like Kryo and Jackson:
 * <pre>
 *   {@code
 *   client.serializer().register(SetCommand.class, new GenericKryoSerializer());
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface CopycatClient {

  /**
   * Indicates the state of the client's communication with the Copycat cluster.
   * <p>
   * Throughout the lifetime of a client, the client will transition through various states according to its
   * ability to communicate with the cluster within the context of a {@link Session}. In some cases, client
   * state changes may be indicative of a loss of guarantees. Users of the client should
   * {@link CopycatClient#onStateChange(Consumer) watch the state of the client} to determine when guarantees
   * are lost and react to changes in the client's ability to communicate with the cluster.
   * <p>
   * <pre>
   *   {@code
   *   client.onStateChange(state -> {
   *     switch (state) {
   *       case CONNECTED:
   *         // The client is healthy
   *         break;
   *       case SUSPENDED:
   *         // The client cannot connect to the cluster and operations may be unsafe
   *         break;
   *       case CLOSED:
   *         // The client has been closed and pending operations have failed
   *         break;
   *     }
   *   });
   *   }
   * </pre>
   * So long as the client is in the {@link #CONNECTED} state, all guarantees with respect to reads and writes will
   * be maintained, and a loss of the {@code CONNECTED} state may indicate a loss of linearizability. See the specific
   * states for more info.
   */
  enum State {

    /**
     * Indicates that the client is connected and its session is open.
     */
    CONNECTED,

    /**
     * Indicates that the client is suspended and its session may or may not be expired.
     */
    SUSPENDED,

    /**
     * Indicates that the client is closed.
     */
    CLOSED

  }

  /**
   * Returns a new Copycat client builder.
   * <p>
   * The provided set of members will be used to connect to the Copycat cluster. The members list does not have to represent
   * the complete list of servers in the cluster, but it must have at least one reachable member that can communicate with
   * the cluster's leader.
   *
   * @return The client builder.
   */
  @SuppressWarnings("unchecked")
  static Builder builder() {
    return builder(Collections.EMPTY_LIST);
  }

  /**
   * Returns a new Copycat client builder.
   * <p>
   * The provided set of members will be used to connect to the Copycat cluster. The members list does not have to represent
   * the complete list of servers in the cluster, but it must have at least one reachable member that can communicate with
   * the cluster's leader.
   *
   * @param cluster The cluster to which to connect.
   * @return The client builder.
   */
  static Builder builder(Address... cluster) {
    return builder(Arrays.asList(cluster));
  }

  /**
   * Returns a new Copycat client builder.
   * <p>
   * The provided set of members will be used to connect to the Copycat cluster. The members list does not have to represent
   * the complete list of servers in the cluster, but it must have at least one reachable member that can communicate with
   * the cluster's leader.
   *
   * @param cluster The cluster to which to connect.
   * @return The client builder.
   */
  static Builder builder(Collection<Address> cluster) {
    return new Builder(cluster);
  }

  /**
   * Returns the current client state.
   * <p>
   * The client's {@link State} is indicative of the client's ability to communicate with the cluster at any given
   * time. Users of the client should use the state to determine when guarantees may be lost. See the {@link State}
   * documentation for information on the specific states.
   *
   * @return The current client state.
   */
  State state();

  /**
   * Registers a callback to be called when the client's state changes.
   *
   * @param callback The callback to be called when the client's state changes.
   * @return The client state change listener.
   */
  Listener<State> onStateChange(Consumer<State> callback);

  /**
   * Returns the client execution context.
   * <p>
   * The thread context is the event loop that this client uses to communicate with Copycat servers.
   * Implementations must guarantee that all asynchronous {@link CompletableFuture} callbacks are
   * executed on a single thread via the returned {@link ThreadContext}.
   * <p>
   * The {@link ThreadContext} can also be used to access the Copycat client's internal
   * {@link Serializer serializer} via {@link ThreadContext#serializer()}.
   *
   * @return The client thread context.
   */
  ThreadContext context();

  /**
   * Returns the client serializer.
   * <p>
   * The serializer can be used to manually register serializable types for submitted {@link Command commands} and
   * {@link Query queries}.
   * <pre>
   *   {@code
   *     client.serializer().register(MyObject.class, 1);
   *     client.serializer().register(MyOtherObject.class, new MyOtherObjectSerializer(), 2);
   *   }
   * </pre>
   *
   * @return The client operation serializer.
   */
  Serializer serializer();

  /**
   * Returns a new session builder.
   *
   * @return A new session builder.
   */
  CopycatSession.Builder sessionBuilder();

  /**
   * Connects the client to Copycat cluster via the default server address.
   * <p>
   * If the client was built with a default cluster list, the default server addresses will be used. Otherwise, the client
   * will attempt to connect to localhost:8700.
   * <p>
   * When the client is connected, it will attempt to connect to and register a session with each unique configured server
   * {@link Address}. Once the session is registered, the client will transition to the {@link State#CONNECTED} state and the
   * returned {@link CompletableFuture} will be completed.
   * <p>
   * The client will connect to servers in the cluster according to the pattern specified by the configured
   * {@link CommunicationStrategy}.
   * <p>
   * In the event that the client is unable to register a session through any of the servers listed in the provided
   * {@link Address} list, the client will use the configured {@link ConnectionStrategy} to determine whether and when
   * to retry the registration attempt.
   *
   * @return A completable future to be completed once the client is registered.
   */
  default CompletableFuture<CopycatClient> connect() {
    return connect((Collection<Address>) null);
  }

  /**
   * Connects the client to Copycat cluster via the provided server addresses.
   * <p>
   * When the client is connected, it will attempt to connect to and register a session with each unique configured server
   * {@link Address}. Once the session is registered, the client will transition to the {@link State#CONNECTED} state and the
   * returned {@link CompletableFuture} will be completed.
   * <p>
   * The client will connect to servers in the cluster according to the pattern specified by the configured
   * {@link CommunicationStrategy}.
   * <p>
   * In the event that the client is unable to register a session through any of the servers listed in the provided
   * {@link Address} list, the client will use the configured {@link ConnectionStrategy} to determine whether and when
   * to retry the registration attempt.
   *
   * @param members A set of server addresses to which to connect.
   * @return A completable future to be completed once the client is registered.
   */
  default CompletableFuture<CopycatClient> connect(Address... members) {
    if (members == null || members.length == 0) {
      return connect();
    } else {
      return connect(Arrays.asList(members));
    }
  }

  /**
   * Connects the client to Copycat cluster via the provided server addresses.
   * <p>
   * When the client is connected, it will attempt to connect to and register a session with each unique configured server
   * {@link Address}. Once the session is registered, the client will transition to the {@link State#CONNECTED} state and the
   * returned {@link CompletableFuture} will be completed.
   * <p>
   * The client will connect to servers in the cluster according to the pattern specified by the configured
   * {@link CommunicationStrategy}.
   * <p>
   * In the event that the client is unable to register a session through any of the servers listed in the provided
   * {@link Address} list, the client will use the configured {@link ConnectionStrategy} to determine whether and when
   * to retry the registration attempt.
   *
   * @param members A set of server addresses to which to connect.
   * @return A completable future to be completed once the client is registered.
   */
  CompletableFuture<CopycatClient> connect(Collection<Address> members);

  /**
   * Closes the client.
   * <p>
   * Closing the client will cause the client to unregister its current {@link Session} if registered. Once the
   * client's session is unregistered, the returned {@link CompletableFuture} will be completed. If the client
   * is unable to unregister its session, the client will transition to the {@link State#SUSPENDED} state and
   * continue to attempt to reconnect and unregister its session until it is able to unregister its session or
   * determine that it was already expired by the cluster.
   *
   * @return A completable future to be completed once the client has been closed.
   */
  CompletableFuture<Void> close();

  /**
   * Builds a new Copycat client.
   * <p>
   * New client builders should be constructed using the static {@link #builder()} factory method.
   * <pre>
   *   {@code
   *     CopycatClient client = CopycatClient.builder(new Address("123.456.789.0", 5000), new Address("123.456.789.1", 5000)
   *       .withTransport(new NettyTransport())
   *       .build();
   *   }
   * </pre>
   */
  final class Builder implements io.atomix.catalyst.util.Builder<CopycatClient> {
    private final Collection<Address> cluster;
    private String clientId = UUID.randomUUID().toString();
    private Transport transport;
    private Serializer serializer;
    private Duration sessionTimeout = Duration.ZERO;
    private Duration unsableTimeout = Duration.ZERO;
    private int threadPoolSize = Runtime.getRuntime().availableProcessors();
    private ConnectionStrategy connectionStrategy = ConnectionStrategies.ONCE;
    private CommunicationStrategy communicationStrategy = CommunicationStrategies.ANY;

    private Builder(Collection<Address> cluster) {
      this.cluster = Assert.notNull(cluster, "cluster");
    }

    /**
     * Sets the client ID.
     * <p>
     * The client ID is a name that should be unique among all clients. The ID will be used to resolve
     * and recover sessions.
     *
     * @param clientId The client ID.
     * @return The client builder.
     * @throws NullPointerException if {@code clientId} is null
     */
    public Builder withClientId(String clientId) {
      this.clientId = Assert.notNull(clientId, "clientId");
      return this;
    }

    /**
     * Sets the client transport.
     * <p>
     * By default, the client will use the {@code NettyTransport} with an event loop pool equal to
     * {@link Runtime#availableProcessors()}.
     *
     * @param transport The client transport.
     * @return The client builder.
     * @throws NullPointerException if {@code transport} is null
     */
    public Builder withTransport(Transport transport) {
      this.transport = Assert.notNull(transport, "transport");
      return this;
    }

    /**
     * Sets the client serializer.
     *
     * @param serializer The client serializer.
     * @return The client builder.
     * @throws NullPointerException if {@code serializer} is null
     */
    public Builder withSerializer(Serializer serializer) {
      this.serializer = Assert.notNull(serializer, "serializer");
      return this;
    }

    /**
     * Sets the client thread pool size.
     *
     * @param threadPoolSize The client thread pool size.
     * @return The client builder.
     * @throws IllegalArgumentException if the thread pool size is not positive
     */
    public Builder withThreadPoolSize(int threadPoolSize) {
      this.threadPoolSize = Assert.argNot(threadPoolSize, threadPoolSize <= 0, "threadPoolSize must be positive");
      return this;
    }

    /**
     * Sets the client session timeout.
     *
     * @param sessionTimeout The client's session timeout.
     * @return The client builder.
     * @throws NullPointerException if the session timeout is null
     * @throws IllegalArgumentException if the session timeout is not {@code -1} or positive
     */
    public Builder withSessionTimeout(Duration sessionTimeout) {
      this.sessionTimeout = Assert.arg(Assert.notNull(sessionTimeout, "sessionTimeout"), sessionTimeout.toMillis() >= -1, "session timeout must be positive or -1");
      return this;
    }

    /**
     * Sets the timeout for session unstability. Client is automatically closed if it can not reach servers within
     * the given timeout.
     *
     * @param unstabilityTimeout The client's unstability timeout.
     * @return The client builder.
     * @throws NullPointerException if the unstability timeout is null
     * @throws IllegalArgumentException if the unstability timeout is not positive
     */
    public Builder withUnstableTimeout(Duration unstabilityTimeout) {
      this.unsableTimeout = Assert.arg(
          Assert.notNull(unstabilityTimeout, "unstabilityTimeout"),
          unstabilityTimeout.toMillis() > 0,
          "unstability timeout must be positive"
      );
      return this;
    }

    /**
     * Sets the client connection strategy.
     *
     * @param connectionStrategy The client connection strategy.
     * @return The client builder.
     * @throws NullPointerException If the connection strategy is {@code null}
     */
    public Builder withConnectionStrategy(ConnectionStrategy connectionStrategy) {
      this.connectionStrategy = Assert.notNull(connectionStrategy, "connectionStrategy");
      return this;
    }

    /**
     * Sets the server selection strategy.
     *
     * @param communicationStrategy The server selection strategy.
     * @return The client builder.
     */
    public Builder withServerSelectionStrategy(CommunicationStrategy communicationStrategy) {
      this.communicationStrategy = Assert.notNull(communicationStrategy, "serverSelectionStrategy");
      return this;
    }

    /**
     * @throws ConfigurationException if transport is not configured and {@code io.atomix.catalyst.transport.netty.NettyTransport}
     * is not found on the classpath
     */
    @Override
    public CopycatClient build() {
      // If the transport is not configured, attempt to use the default Netty transport.
      if (transport == null) {
        try {
          transport = (Transport) Class.forName("io.atomix.catalyst.transport.netty.NettyTransport").newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
          throw new ConfigurationException("transport not configured");
        }
      }

      // If no serializer instance was provided, create one.
      if (serializer == null) {
        serializer = new Serializer();
      }

      ScheduledExecutorService executor = Executors.newScheduledThreadPool(threadPoolSize, new CatalystThreadFactory("copycat-client-%d"));

      // Add service loader types to the primary serializer.
      serializer.resolve(new ClientRequestTypeResolver());
      serializer.resolve(new ClientResponseTypeResolver());
      serializer.resolve(new ProtocolSerialization());

      return new DefaultCopycatClient(
        clientId,
        cluster,
        transport.client(),
        executor,
        serializer,
        connectionStrategy,
        sessionTimeout,
        unsableTimeout
      );
    }
  }

}
