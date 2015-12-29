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
package io.atomix.copycat.client.util;

import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.MessageHandler;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.copycat.client.request.ConnectRequest;
import io.atomix.copycat.client.request.Request;
import io.atomix.copycat.client.response.ConnectResponse;
import io.atomix.copycat.client.response.Response;

import java.net.ConnectException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Client connection that recursively connects to servers in the cluster and attempts to submit requests.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ClientConnection implements Connection {
  private final UUID id;
  private final Client client;
  private final AddressSelector selector;
  private CompletableFuture<Connection> connectFuture;
  private final Map<Class<?>, MessageHandler<?, ?>> handlers = new ConcurrentHashMap<>();
  private Connection connection;
  private boolean open = true;

  public ClientConnection(UUID id, Client client, AddressSelector selector) {
    this.id = Assert.notNull(id, "id");
    this.client = Assert.notNull(client, "client");
    this.selector = Assert.notNull(selector, "selector");
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T, U> CompletableFuture<U> send(T request) {
    CompletableFuture<U> future = new CompletableFuture<>();
    sendRequest((Request) request, (CompletableFuture<Response>) future);
    return future;
  }

  /**
   * Sends the given request attempt to the cluster.
   */
  private <T extends Request<T>, U extends Response<U>> void sendRequest(T request, CompletableFuture<U> future) {
    if (open) {
      connect().whenComplete((c, e) -> sendRequest(request, c, e, future));
    }
  }

  /**
   * Sends the given request attempt to the cluster via the given connection if connected.
   */
  private <T extends Request<T>, U extends Response<U>> void sendRequest(T request, Connection connection, Throwable error, CompletableFuture<U> future) {
    if (open) {
      if (error == null) {
        connection.<T, U>send(request).whenComplete((r, e) -> handleResponse(request, r, e, future));
      } else {
        next().whenComplete((c, e) -> sendRequest(request, c, e, future));
      }
    }
  }

  /**
   * Handles a response from the cluster.
   */
  private <T extends Request<T>, U extends Response<U>> void handleResponse(T request, U response, Throwable error, CompletableFuture<U> future) {
    if (open) {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete(response);
        } else {
          next().whenComplete((c, e) -> sendRequest(request, c, e, future));
        }
      } else {
        next().whenComplete((c, e) -> sendRequest(request, c, e, future));
      }
    }
  }

  /**
   * Connects to the cluster.
   */
  private CompletableFuture<Connection> connect() {
    // If the address selector has been reset then reset the connection.
    if (selector.state() == AddressSelector.State.RESET)
      connection = null;

    // If a connection was already established then use that connection.
    if (connection != null)
      return CompletableFuture.completedFuture(connection);

    // If a connection is currently being established then piggyback on the connect future.
    if (connectFuture != null)
      return connectFuture;

    // Create a new connect future and connect to the first server in the cluster.
    connectFuture = new CompletableFuture<>();
    connect(connectFuture);

    // Reset the connect future field once the connection is complete.
    return connectFuture.whenComplete((result, error) -> connectFuture = null);
  }

  /**
   * Connects to the cluster using the next connection.
   */
  private CompletableFuture<Connection> next() {
    connection = null;
    return connect();
  }

  /**
   * Attempts to connect to the cluster.
   */
  private void connect(CompletableFuture<Connection> future) {
    if (!selector.hasNext()) {
      future.completeExceptionally(new ConnectException("failed to connect to the cluster"));
    } else {
      client.connect(selector.next()).whenComplete((c, e) -> handleConnection(c, e, future));
    }
  }

  /**
   * Handles a connection to a server.
   */
  private void handleConnection(Connection connection, Throwable error, CompletableFuture<Connection> future) {
    if (open) {
      if (error == null) {
        setupConnection(connection, future);
      } else {
        connect(future);
      }
    }
  }

  /**
   * Sets up the given connection.
   */
  @SuppressWarnings("unchecked")
  private void setupConnection(Connection connection, CompletableFuture<Connection> future) {
    this.connection = connection;
    connection.closeListener(c -> {
      if (c.equals(this.connection)) {
        this.connection = null;
      }
    });
    connection.exceptionListener(c -> {
      if (c.equals(this.connection)) {
        this.connection = null;
      }
    });

    for (Map.Entry<Class<?>, MessageHandler<?, ?>> entry : handlers.entrySet()) {
      connection.handler((Class) entry.getKey(), entry.getValue());
    }

    // When we first connect to a new server, first send a ConnectRequest to the server to establish
    // the connection with the server-side state machine.
    ConnectRequest request = ConnectRequest.builder()
      .withClientId(id)
      .build();

    connection.<ConnectRequest, ConnectResponse>send(request).whenComplete((r, e) -> handleConnectResponse(r, e, future));
  }

  /**
   * Handles a connect response.
   */
  private void handleConnectResponse(ConnectResponse response, Throwable error, CompletableFuture<Connection> future) {
    if (open && connectFuture != null) {
      if (error == null) {
        // If the connection was successfully created, immediately send a keep-alive request
        // to the server to ensure we maintain our session and get an updated list of server addresses.
        if (response.status() == Response.Status.OK) {
          future.complete(connection);
        } else {
          connect(future);
        }
      } else {
        connect(future);
      }
    }
  }

  @Override
  public <T, U> Connection handler(Class<T> type, MessageHandler<T, U> handler) {
    Assert.notNull(type, "type");
    Assert.notNull(handler, "handler");
    handlers.put(type, handler);
    if (connection != null)
      connection.handler(type, handler);
    return this;
  }

  @Override
  public Listener<Throwable> exceptionListener(Consumer<Throwable> listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Listener<Connection> closeListener(Consumer<Connection> listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Void> close() {
    open = false;
    return CompletableFuture.completedFuture(null);
  }

}
