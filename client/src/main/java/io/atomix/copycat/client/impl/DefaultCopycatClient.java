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

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.concurrent.ThreadPoolContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.ConnectionStrategy;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.CopycatMetadata;
import io.atomix.copycat.client.session.CopycatSession;
import io.atomix.copycat.client.session.impl.CopycatSessionManager;
import io.atomix.copycat.client.util.AddressSelectorManager;
import io.atomix.copycat.client.util.ClientConnectionManager;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

/**
 * Default Copycat client implementation.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class DefaultCopycatClient implements CopycatClient {
  private static final String DEFAULT_HOST = "0.0.0.0";
  private static final int DEFAULT_PORT = 8700;
  private final Collection<Address> cluster;
  private final CopycatClientState state;
  private final ThreadContext threadContext;
  private final ClientConnectionManager connectionManager;
  private final CopycatMetadata metadata;
  private final AddressSelectorManager selectorManager = new AddressSelectorManager();
  private final CopycatSessionManager sessionManager;
  private volatile CompletableFuture<CopycatClient> openFuture;
  private volatile CompletableFuture<Void> closeFuture;

  public DefaultCopycatClient(String clientId, Collection<Address> cluster, Client client, ScheduledExecutorService threadPoolExecutor, Serializer serializer, ConnectionStrategy connectionStrategy, Duration sessionTimeout, Duration unstableTimeout) {
    this.cluster = Assert.notNull(cluster, "cluster");
    this.threadContext = new ThreadPoolContext(threadPoolExecutor, serializer.clone());
    this.state = new CopycatClientState(clientId);
    this.connectionManager = new ClientConnectionManager(client);
    this.metadata = new DefaultCopycatMetadata(connectionManager, selectorManager);
    this.sessionManager = new CopycatSessionManager(state, connectionManager, selectorManager, threadContext, threadPoolExecutor, connectionStrategy, sessionTimeout, unstableTimeout);
  }

  @Override
  public State state() {
    return state.getState();
  }

  @Override
  public Listener<State> onStateChange(Consumer<State> callback) {
    return state.onStateChange(callback);
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
    if (state.getState() != State.CLOSED)
      return CompletableFuture.completedFuture(this);

    if (openFuture == null) {
      openFuture = new CompletableFuture<>();

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
          openFuture.complete(this);
        } else {
          openFuture.completeExceptionally(error);
        }
      }, threadContext);
    }
    return openFuture;
  }

  @Override
  public CopycatSession.Builder sessionBuilder() {
    return new SessionBuilder();
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (state.getState() == State.CLOSED)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      closeFuture = sessionManager.close().whenComplete((r, e) -> connectionManager.close());
    }
    return closeFuture;
  }

  /**
   * Kills the client.
   *
   * @return A completable future to be completed once the client's session has been killed.
   */
  public synchronized CompletableFuture<Void> kill() {
    if (state.getState() == State.CLOSED)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      closeFuture = sessionManager.kill();
    }
    return closeFuture;
  }

  @Override
  public int hashCode() {
    return 23 + 37 * state.getUuid().hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof DefaultCopycatClient && ((DefaultCopycatClient) object).state.getUuid().equals(state.getUuid());
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d, uuid=%s]", getClass().getSimpleName(), state.getId(), state.getUuid());
  }

  /**
   * Default Copycat session builder.
   */
  private class SessionBuilder extends CopycatSession.Builder {
    @Override
    public CopycatSession build() {
      return sessionManager.openSession(name, type, communicationStrategy).join();
    }
  }

}
