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
package io.atomix.copycat.client;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.ServiceLoaderTypeResolver;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ConfigurationException;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.session.ClientSession;
import io.atomix.copycat.client.session.Session;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * Provides a feature complex {@link RaftClient}.
 * <p>
 * Copycat clients can be constructed using the {@link CopycatClient.Builder}. To create a new client builder, use the
 * static {@link #builder(Address...)} method, passing one or more server {@link Address}:
 * <pre>
 *   {@code
 *     CopycatClient client = CopycatClient.builder(new Address("123.456.789.0", 5000), new Address("123.456.789.1", 5000).build();
 *   }
 * </pre>
 * By default, the client will attempt to use the {@code NettyTransport} to communicate with the cluster. See the {@link CopycatClient.Builder}
 * documentation for client configuration options.
 * <p>
 * Copycat clients interact with one or more nodes in a cluster through a session. When the client is {@link #open() opened},
 * the client will attempt to one of the known member {@link Address} provided to the builder. As long as the client can
 * communicate with at least one correct member of the cluster, it can open a session. Once the client is able to register a
 * {@link Session}, it will receive an updated list of members for the entire cluster and thereafter be allowed to communicate
 * with all servers.
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
 * the cluster through the {@link #submit(Command)} and {@link #submit(Query)} methods respectively:
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
 * Sessions work to provide linearizable semantics for client {@link Command commands}. When a command is submitted to the cluster,
 * the command will be forwarded to the leader where it will be logged and replicated. Once the command is stored on a majority
 * of servers, the leader will apply it to its state machine and respond according to the command's {@link Command#consistency()}.
 * See the {@link Command.ConsistencyLevel} documentation for more info.
 * <p>
 * Sessions also allow {@link Query queries} (read-only requests) submitted by the client to optionally be executed on follower
 * nodes. When a query is submitted to the cluster, the query's {@link Query#consistency()} will be used to determine how the
 * query is handled. For queries with stronger consistency levels, they will be forwarded to the cluster's leader. For weaker
 * consistency queries, they may be executed on follower nodes according to the consistency level constraints. See the
 * {@link Query.ConsistencyLevel} documentation for more info.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopycatClient implements RaftClient {

  /**
   * Returns a new Raft client builder.
   * <p>
   * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
   * the complete list of servers in the cluster, but it must have at least one reachable member that can communicate with
   * the cluster's leader.
   *
   * @param members The cluster members to which to connect.
   * @return The client builder.
   */
  public static Builder builder(Address... members) {
    return builder(Arrays.asList(Assert.notNull(members, "members")));
  }

  /**
   * Returns a new Raft client builder.
   * <p>
   * The provided set of members will be used to connect to the Raft cluster. The members list does not have to represent
   * the complete list of servers in the cluster, but it must have at least one reachable member that can communicate with
   * the cluster's leader.
   *
   * @param members The cluster members to which to connect.
   * @return The client builder.
   */
  public static Builder builder(Collection<Address> members) {
    return new Builder(members);
  }

  private final UUID id = UUID.randomUUID();
  private final Transport transport;
  private final Collection<Address> members;
  private final Serializer serializer;
  private final ConnectionStrategy connectionStrategy;
  private final RecoveryStrategy recoveryStrategy;
  private volatile ClientSession session;
  private volatile CompletableFuture<RaftClient> openFuture;
  private volatile CompletableFuture<Void> closeFuture;

  protected CopycatClient(Transport transport, Collection<Address> members, Serializer serializer, ConnectionStrategy connectionStrategy, RecoveryStrategy recoveryStrategy) {
    serializer.resolve(new ServiceLoaderTypeResolver());
    this.transport = Assert.notNull(transport, "transport");
    this.members = Assert.notNull(members, "members");
    this.serializer = Assert.notNull(serializer, "serializer");
    this.connectionStrategy = Assert.notNull(connectionStrategy, "connectionStrategy");
    this.recoveryStrategy = Assert.notNull(recoveryStrategy, "recoveryStrategy");
  }

  @Override
  public ThreadContext context() {
    return session != null ? session.context() : null;
  }

  @Override
  public Transport transport() {
    return transport;
  }

  @Override
  public Serializer serializer() {
    return serializer;
  }

  @Override
  public Session session() {
    return session;
  }

  @Override
  public <T> CompletableFuture<T> submit(Command<T> command) {
    Assert.notNull(command, "command");
    if (session == null)
      return Futures.exceptionalFuture(new IllegalStateException("client not open"));
    return session.submit(command);
  }

  @Override
  public <T> CompletableFuture<T> submit(Query<T> query) {
    Assert.notNull(query, "query");
    if (session == null)
      return Futures.exceptionalFuture(new IllegalStateException("client not open"));
    return session.submit(query);
  }

  @Override
  public CompletableFuture<RaftClient> open() {
    if (session != null && session.isOpen())
      return CompletableFuture.completedFuture(this);

    if (openFuture == null) {
      synchronized (this) {
        if (openFuture == null) {
          ClientSession session = new ClientSession(id, transport, members, serializer, connectionStrategy);
          if (closeFuture == null) {
            openFuture = session.open().thenApply(s -> {
              synchronized (this) {
                openFuture = null;
                this.session = session;
                registerStrategies(session);
                return this;
              }
            });
          } else {
            openFuture = closeFuture.thenCompose(v -> session.open().thenApply(s -> {
              synchronized (this) {
                openFuture = null;
                this.session = session;
                registerStrategies(session);
                return this;
              }
            }));
          }
        }
      }
    }
    return openFuture;
  }

  @Override
  public boolean isOpen() {
    return session != null && session.isOpen();
  }

  /**
   * Registers strategies on the client's session.
   */
  private void registerStrategies(Session session) {
    session.onClose(s -> {
      this.session = null;
      if (s.isExpired()) {
        recoveryStrategy.recover(this);
      }
    });
  }

  @Override
  public CompletableFuture<Void> close() {
    if (session == null || !session.isOpen())
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      synchronized (this) {
        if (session == null) {
          return CompletableFuture.completedFuture(null);
        }

        if (closeFuture == null) {
          if (openFuture == null) {
            closeFuture = session.close().whenComplete((result, error) -> {
              synchronized (this) {
                session = null;
                closeFuture = null;
              }
            });
          } else {
            closeFuture = openFuture.thenCompose(v -> session.close().whenComplete((result, error) -> {
              synchronized (this) {
                session = null;
                closeFuture = null;
              }
            }));
          }
        }
      }
    }
    return closeFuture;
  }

  @Override
  public boolean isClosed() {
    return session == null || session.isClosed();
  }

  /**
   * Builds a new Copycat client.
   * <p>
   * New client builders should be constructed using the static {@link #builder(Address...)} factory method.
   * <pre>
   *   {@code
   *     CopycatClient client = CopycatClient.builder(new Address("123.456.789.0", 5000), new Address("123.456.789.1", 5000)
   *       .withTransport(new NettyTransport())
   *       .build();
   *   }
   * </pre>
   */
  public static class Builder extends io.atomix.catalyst.util.Builder<CopycatClient> {
    private Transport transport;
    private Serializer serializer;
    private Set<Address> members;
    private ConnectionStrategy connectionStrategy = ConnectionStrategies.FOLLOWERS;
    private RecoveryStrategy recoveryStrategy = RecoveryStrategies.CLOSE;

    private Builder(Collection<Address> members) {
      this.members = new HashSet<>(Assert.notNull(members, "members"));
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
     * <p>
     * By default, the client will use a {@link Serializer} configured with the {@link ServiceLoaderTypeResolver}.
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
     * Sets the client connection strategy.
     *
     * @param connectionStrategy The client connection strategy.
     * @return The client builder.
     */
    public Builder withConnectionStrategy(ConnectionStrategy connectionStrategy) {
      this.connectionStrategy = Assert.notNull(connectionStrategy, "connectionStrategy");
      return this;
    }

    /**
     * Sets the client recovery strategy.
     *
     * @param recoveryStrategy The client recovery strategy.
     * @return The client builder.
     */
    public Builder withRecoveryStrategy(RecoveryStrategy recoveryStrategy) {
      this.recoveryStrategy = Assert.notNull(recoveryStrategy, "recoveryStrategy");
      return this;
    }

    /**
     * @throws ConfigurationException if transport is not configured and {@code io.atomix.catalyst.transport.NettyTransport}
     * is not found on the classpath
     */
    @Override
    public CopycatClient build() {
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
        serializer = new Serializer();
      }
      return new CopycatClient(transport, members, serializer, connectionStrategy, recoveryStrategy);
    }
  }

}
