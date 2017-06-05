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
import io.atomix.copycat.util.ProtocolSerialization;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Provides an interface for submitting {@link Command commands} and {@link Query} queries to the Copycat cluster.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface CopycatClient {

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
   * Returns the Copycat metadata.
   *
   * @return The Copycat metadata.
   */
  CopycatMetadata metadata();

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
   * The client will connect to servers in the cluster according to the pattern specified by the configured
   * {@link CommunicationStrategy}.
   *
   * @return A completable future to be completed once the client is registered.
   */
  default CompletableFuture<CopycatClient> connect() {
    return connect((Collection<Address>) null);
  }

  /**
   * Connects the client to Copycat cluster via the provided server addresses.
   * <p>
   * The client will connect to servers in the cluster according to the pattern specified by the configured
   * {@link CommunicationStrategy}.
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
   * The client will connect to servers in the cluster according to the pattern specified by the configured
   * {@link CommunicationStrategy}.
   *
   * @param members A set of server addresses to which to connect.
   * @return A completable future to be completed once the client is registered.
   */
  CompletableFuture<CopycatClient> connect(Collection<Address> members);

  /**
   * Closes the client.
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
    private int threadPoolSize = Runtime.getRuntime().availableProcessors();

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
        serializer
      );
    }
  }

}
