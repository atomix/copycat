/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.copycat.client.impl;

import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.concurrent.ThreadPoolContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.CopycatMetadata;
import io.atomix.copycat.client.session.CopycatSession;
import io.atomix.copycat.client.session.impl.CopycatSessionManager;
import io.atomix.copycat.client.util.AddressSelectorManager;
import io.atomix.copycat.client.util.ClientConnectionManager;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Default Copycat client implementation.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class DefaultCopycatClient implements CopycatClient {
  private static final String DEFAULT_HOST = "0.0.0.0";
  private static final int DEFAULT_PORT = 8700;
  private final String id;
  private final Collection<Address> cluster;
  private final ThreadContext threadContext;
  private final ClientConnectionManager connectionManager;
  private final CopycatMetadata metadata;
  private final AddressSelectorManager selectorManager = new AddressSelectorManager();
  private final CopycatSessionManager sessionManager;

  public DefaultCopycatClient(
    String clientId,
    Collection<Address> cluster,
    Client client,
    ScheduledExecutorService threadPoolExecutor,
    Serializer serializer) {
    this.id = Assert.notNull(clientId, "clientId");
    this.cluster = Assert.notNull(cluster, "cluster");
    this.threadContext = new ThreadPoolContext(threadPoolExecutor, serializer.clone());
    this.connectionManager = new ClientConnectionManager(client);
    this.metadata = new DefaultCopycatMetadata(clientId, connectionManager, selectorManager);
    this.sessionManager = new CopycatSessionManager(clientId, connectionManager, selectorManager, threadContext, threadPoolExecutor);
  }

  @Override
  public String id() {
    return id;
  }

  @Override
  public CopycatMetadata metadata() {
    return metadata;
  }

  @Override
  public Serializer serializer() {
    return threadContext.serializer();
  }

  @Override
  public ThreadContext context() {
    return threadContext;
  }

  @Override
  public synchronized CompletableFuture<CopycatClient> connect(Collection<Address> cluster) {
    CompletableFuture<CopycatClient> future = new CompletableFuture<>();

    // If the provided cluster list is null or empty, use the default list.
    if (cluster == null || cluster.isEmpty()) {
      cluster = this.cluster;
    }

    // If the default list is null or empty, use the default host:port.
    if (cluster == null || cluster.isEmpty()) {
      cluster = Collections.singletonList(new Address(DEFAULT_HOST, DEFAULT_PORT));
    }

    // Reset the connection list to allow the selection strategy to prioritize connections.
    sessionManager.resetConnections(null, cluster);

    // Register the session manager.
    sessionManager.open().whenCompleteAsync((result, error) -> {
      if (error == null) {
        future.complete(this);
      } else {
        future.completeExceptionally(error);
      }
    }, threadContext);
    return future;
  }

  @Override
  public CopycatSession.Builder sessionBuilder() {
    return new SessionBuilder();
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return sessionManager.close()
      .whenComplete((e, r) -> {
        connectionManager.close();
        threadContext.close();
      });
  }

  @Override
  public int hashCode() {
    return 23 + 37 * id.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof DefaultCopycatClient && ((DefaultCopycatClient) object).id.equals(id);
  }

  @Override
  public String toString() {
    return String.format("%s[id=%s]", getClass().getSimpleName(), id);
  }

  /**
   * Default Copycat session builder.
   */
  private class SessionBuilder extends CopycatSession.Builder {
    @Override
    public CopycatSession build() {
      return sessionManager.openSession(name, type, communicationStrategy, timeout).join();
    }
  }

}
