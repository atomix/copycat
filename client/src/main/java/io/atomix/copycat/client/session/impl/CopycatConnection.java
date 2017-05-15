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
package io.atomix.copycat.client.session.impl;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.TransportException;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.util.AddressSelector;
import io.atomix.copycat.client.util.ClientConnectionManager;
import io.atomix.copycat.client.util.OrderedCompletableFuture;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.Request;
import io.atomix.copycat.protocol.Response;
import org.slf4j.Logger;

import java.net.ConnectException;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Client connection that recursively connects to servers in the cluster and attempts to submit requests.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class CopycatConnection {
  protected final ClientConnectionManager connections;
  protected final AddressSelector selector;
  protected CompletableFuture<Connection> connectFuture;
  protected final Map<String, Function> handlers = new ConcurrentHashMap<>();
  protected Connection connection;
  protected volatile boolean open;

  public CopycatConnection(ClientConnectionManager connections, AddressSelector selector) {
    this.connections = Assert.notNull(connections, "connections");
    this.selector = Assert.notNull(selector, "selector");
  }

  /**
   * Returns the connection name.
   *
   * @return The connection name.
   */
  protected abstract String name();

  /**
   * Returns the connection logger.
   *
   * @return The connection logger.
   */
  protected abstract Logger logger();

  /**
   * Returns the current selector leader.
   *
   * @return The current selector leader.
   */
  public Address leader() {
    return selector.leader();
  }

  /**
   * Returns the current set of servers.
   *
   * @return The current set of servers.
   */
  public Collection<Address> servers() {
    return selector.servers();
  }

  /**
   * Resets the client connection.
   *
   * @return The client connection.
   */
  public CopycatConnection reset() {
    selector.reset();
    return this;
  }

  /**
   * Resets the client connection.
   *
   * @param leader The current cluster leader.
   * @param servers The current servers.
   * @return The client connection.
   */
  public CopycatConnection reset(Address leader, Collection<Address> servers) {
    selector.reset(leader, servers);
    return this;
  }

  /**
   * Opens the connection.
   *
   * @return A completable future to be completed once the connection is opened.
   */
  public CompletableFuture<Void> open() {
    open = true;
    return connect().thenApply(c -> null);
  }

  /**
   * Sends a request to the cluster.
   *
   * @param type The request type.
   * @param request The request to send.
   * @return A completable future to be completed with the response.
   */
  public CompletableFuture<Void> send(String type, Object request) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    sendRequest((Request) request, (r, c) -> c.send(type, r), future);
    return future;
  }

  /**
   * Sends a request to the cluster and awaits a response.
   *
   * @param type The request type.
   * @param request The request to send.
   * @param <T> The request type.
   * @param <U> The response type.
   * @return A completable future to be completed with the response.
   */
  public <T, U> CompletableFuture<U> sendAndReceive(String type, T request) {
    CompletableFuture<U> future = new CompletableFuture<>();
    sendRequest((Request) request, (r, c) -> c.sendAndReceive(type, r), future);
    return future;
  }

  /**
   * Sends the given request attempt to the cluster.
   */
  protected <T extends Request, U> void sendRequest(T request, BiFunction<Request, Connection, CompletableFuture<U>> sender, CompletableFuture<U> future) {
    if (open) {
      connect().whenComplete((c, e) -> sendRequest(request, sender, c, e, future));
    }
  }

  /**
   * Sends the given request attempt to the cluster via the given connection if connected.
   */
  protected <T extends Request, U> void sendRequest(T request, BiFunction<Request, Connection, CompletableFuture<U>> sender, Connection connection, Throwable error, CompletableFuture<U> future) {
    if (error == null) {
      if (connection != null) {
        logger().trace("{} - Sending {}", name(), request);
        sender.apply(request, connection).whenComplete((r, e) -> {
          if (e != null || r != null) {
            handleResponse(request, sender, connection, (Response) r, e, future);
          } else {
            future.complete(null);
          }
        });
      } else {
        future.completeExceptionally(new ConnectException("Failed to connect to the cluster"));
      }
    } else {
      logger().trace("{} - Resending {}: {}", name(), request, error);
      resendRequest(error, request, sender, connection, future);
    }
  }

  /**
   * Resends a request due to a request failure, resetting the connection if necessary.
   */
  @SuppressWarnings("unchecked")
  protected <T extends Request> void resendRequest(Throwable cause, T request, BiFunction sender, Connection connection, CompletableFuture future) {
    // If the connection has not changed, reset it and connect to the next server.
    if (this.connection == connection) {
      logger().trace("{} - Resetting connection. Reason: {}", name(), cause);
      this.connection = null;
    }

    // Create a new connection and resend the request. This will force retries to piggyback on any existing
    // connect attempts.
    connect().whenComplete((c, e) -> sendRequest(request, sender, c, e, future));
  }

  /**
   * Handles a response from the cluster.
   */
  @SuppressWarnings("unchecked")
  protected <T extends Request> void handleResponse(T request, BiFunction sender, Connection connection, Response response, Throwable error, CompletableFuture future) {
    if (error == null) {
      if (response.status() == Response.Status.OK
        || response.error() == CopycatError.Type.COMMAND_ERROR
        || response.error() == CopycatError.Type.QUERY_ERROR
        || response.error() == CopycatError.Type.APPLICATION_ERROR
        || response.error() == CopycatError.Type.UNKNOWN_CLIENT_ERROR
        || response.error() == CopycatError.Type.UNKNOWN_SESSION_ERROR
        || response.error() == CopycatError.Type.UNKNOWN_STATE_MACHINE_ERROR
        || response.error() == CopycatError.Type.INTERNAL_ERROR) {
        logger().trace("{} - Received {}", name(), response);
        future.complete(response);
      } else {
        resendRequest(response.error().createException(), request, sender, connection, future);
      }
    } else if (error instanceof ConnectException || error instanceof TimeoutException || error instanceof TransportException || error instanceof ClosedChannelException) {
      resendRequest(error, request, sender, connection, future);
    } else {
      logger().debug("{} - {} failed! Reason: {}", name(), request, error);
      future.completeExceptionally(error);
    }
  }

  /**
   * Connects to the cluster.
   */
  protected CompletableFuture<Connection> connect() {
    // If the address selector has been reset then reset the connection.
    if (selector.state() == AddressSelector.State.RESET && connection != null) {
      if (connectFuture != null) {
        return connectFuture;
      }

      CompletableFuture<Connection> future = new OrderedCompletableFuture<>();
      future.whenComplete((r, e) -> this.connectFuture = null);
      this.connectFuture = future;

      this.connection = null;
      connect(future);
      return future;
    }

    // If a connection was already established then use that connection.
    if (connection != null) {
      return CompletableFuture.completedFuture(connection);
    }

    // If a connection is currently being established then piggyback on the connect future.
    if (connectFuture != null) {
      return connectFuture;
    }

    // Create a new connect future and connect to the first server in the cluster.
    CompletableFuture<Connection> future = new OrderedCompletableFuture<>();
    future.whenComplete((r, e) -> this.connectFuture = null);
    this.connectFuture = future;
    reset().connect(future);
    return future;
  }

  /**
   * Attempts to connect to the cluster.
   */
  protected void connect(CompletableFuture<Connection> future) {
    if (!selector.hasNext()) {
      logger().debug("{} - Failed to connect to the cluster", name());
      future.complete(null);
    } else {
      Address address = selector.next();
      logger().debug("{} - Connecting to {}", name(), address);
      connections.getConnection(address).whenComplete((c, e) -> handleConnection(address, c, e, future));
    }
  }

  /**
   * Handles a connection to a server.
   */
  protected void handleConnection(Address address, Connection connection, Throwable error, CompletableFuture<Connection> future) {
    if (error == null) {
      setupConnection(address, connection, future);
    } else {
      logger().debug("{} - Failed to connect! Reason: {}", name(), error);
      connect(future);
    }
  }

  /**
   * Sets up the given connection.
   */
  @SuppressWarnings("unchecked")
  protected void setupConnection(Address address, Connection connection, CompletableFuture<Connection> future) {
    logger().debug("{} - Setting up connection to {}", name(), address);

    this.connection = connection;

    connection.onClose(c -> {
      if (c.equals(this.connection)) {
        logger().debug("{} - Connection closed", name());
        this.connection = null;
        connect();
      }
    });
    connection.onException(c -> {
      if (c.equals(this.connection)) {
        logger().debug("{} - Connection lost", name());
        this.connection = null;
        connect();
      }
    });

    for (Map.Entry<String, Function> entry : handlers.entrySet()) {
      connection.registerHandler(entry.getKey(), entry.getValue());
    }
    future.complete(connection);
  }

  /**
   * Registers a handler for the given message type.
   *
   * @param type The message type for which to register the handler.
   * @param handler The handler to register.
   * @param <T> The handler type.
   * @return The client connection.
   */
  @SuppressWarnings("unchecked")
  public <T> CopycatConnection registerHandler(String type, Consumer<T> handler) {
    return registerHandler(type, r -> {
      handler.accept((T) r);
      return null;
    });
  }

  /**
   * Registers a handler for the given message type.
   *
   * @param type The message type for which to register the handler.
   * @param handler The handler to register.
   * @param <T> The handler type.
   * @param <U> The response type.
   * @return The client connection.
   */
  public <T, U> CopycatConnection registerHandler(String type, Function<T, CompletableFuture<U>> handler) {
    Assert.notNull(type, "type");
    Assert.notNull(handler, "handler");
    handlers.put(type, handler);
    if (connection != null)
      connection.registerHandler(type, handler);
    return this;
  }

  /**
   * Closes the connection.
   *
   * @return A completable future to be completed once the connection is closed.
   */
  public CompletableFuture<Void> close() {
    open = false;
    return CompletableFuture.completedFuture(null);
  }

}
