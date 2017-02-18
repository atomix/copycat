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

import io.atomix.copycat.protocol.*;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
import io.atomix.copycat.util.Assert;
import io.atomix.copycat.util.concurrent.Listener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.net.ProtocolException;
import java.nio.channels.ClosedChannelException;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Client connection that recursively connects to servers in the cluster and attempts to submit requests.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ClientConnection implements ProtocolClientConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientConnection.class);
  private final String id;
  private final ProtocolClient client;
  private final AddressSelector selector;
  private CompletableFuture<ProtocolClientConnection> connectFuture;
  private ProtocolListener<PublishRequest, PublishResponse> publishListener;
  private ProtocolClientConnection connection;
  private boolean open = true;

  public ClientConnection(String id, ProtocolClient client, AddressSelector selector) {
    this.id = Assert.notNull(id, "id");
    this.client = Assert.notNull(client, "client");
    this.selector = Assert.notNull(selector, "selector");
  }

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
  public ClientConnection reset() {
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
  public ClientConnection reset(Address leader, Collection<Address> servers) {
    selector.reset(leader, servers);
    return this;
  }

  @Override
  public CompletableFuture<ConnectResponse> connect(ConnectRequest request) {
    CompletableFuture<ConnectResponse> future = new CompletableFuture<>();
    sendRequest(connection -> connection.connect(request), future);
    return future;
  }

  @Override
  public CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    CompletableFuture<RegisterResponse> future = new CompletableFuture<>();
    sendRequest(connection -> connection.register(request), future);
    return future;
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    CompletableFuture<KeepAliveResponse> future = new CompletableFuture<>();
    sendRequest(connection -> connection.keepAlive(request), future);
    return future;
  }

  @Override
  public CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request) {
    CompletableFuture<UnregisterResponse> future = new CompletableFuture<>();
    sendRequest(connection -> connection.unregister(request), future);
    return future;
  }

  @Override
  public CompletableFuture<CommandResponse> command(CommandRequest request) {
    CompletableFuture<CommandResponse> future = new CompletableFuture<>();
    sendRequest(connection -> connection.command(request), future);
    return future;
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    sendRequest(connection -> connection.query(request), future);
    return future;
  }

  @Override
  public ProtocolClientConnection onPublish(ProtocolListener<PublishRequest, PublishResponse> listener) {
    this.publishListener = listener;
    return this;
  }

  /**
   * Sends the given request attempt to the cluster.
   */
  private <T extends ProtocolRequest, U extends ProtocolResponse> void sendRequest(Function<ProtocolClientConnection, CompletableFuture<U>> factory, CompletableFuture<U> future) {
    if (open) {
      connect().whenComplete((c, e) -> sendRequest(factory, c, e, future));
    }
  }

  /**
   * Sends the given request attempt to the cluster via the given connection if connected.
   */
  private <T extends ProtocolRequest, U extends ProtocolResponse> void sendRequest(Function<ProtocolClientConnection, CompletableFuture<U>> factory, ProtocolClientConnection connection, Throwable error, CompletableFuture<U> future) {
    if (open) {
      if (error == null) {
        if (connection != null) {
          factory.apply(connection).whenComplete((r, e) -> handleResponse(factory, r, e, future));
        } else {
          future.completeExceptionally(new ConnectException("failed to connect"));
        }
      } else {
        this.connection = null;
        next().whenComplete((c, e) -> sendRequest(factory, c, e, future));
      }
    }
  }

  /**
   * Handles a response from the cluster.
   */
  private <T extends ProtocolRequest, U extends ProtocolResponse> void handleResponse(Function<ProtocolClientConnection, CompletableFuture<U>> factory, U response, Throwable error, CompletableFuture<U> future) {
    if (open) {
      if (error == null) {
        if (response.status() == ProtocolResponse.Status.OK
          || response.error().type() == ProtocolResponse.Error.Type.COMMAND_ERROR
          || response.error().type() == ProtocolResponse.Error.Type.QUERY_ERROR
          || response.error().type() == ProtocolResponse.Error.Type.APPLICATION_ERROR
          || response.error().type() == ProtocolResponse.Error.Type.UNKNOWN_SESSION_ERROR) {
          future.complete(response);
        } else {
          next().whenComplete((c, e) -> sendRequest(factory, c, e, future));
        }
      } else if (error instanceof ConnectException || error instanceof TimeoutException || error instanceof ProtocolException || error instanceof ClosedChannelException) {
        next().whenComplete((c, e) -> sendRequest(factory, c, e, future));
      } else {
        future.completeExceptionally(error);
      }
    }
  }

  /**
   * Connects to the cluster.
   */
  private CompletableFuture<ProtocolClientConnection> connect() {
    // If the address selector has been then reset the connection.
    if (selector.state() == AddressSelector.State.RESET && connection != null) {
      if (connectFuture != null)
        return connectFuture;

      CompletableFuture<ProtocolClientConnection> future = new CompletableFuture<>();
      connectFuture = future;
      connection.close().whenComplete((result, error) -> connect(future));
      return connectFuture.whenComplete((result, error) -> connectFuture = null);
    }

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
  private CompletableFuture<ProtocolClientConnection> next() {
    if (connection != null)
      return connection.close().thenRun(() -> connection = null).thenCompose(v -> connect());
    return connect();
  }

  /**
   * Attempts to connect to the cluster.
   */
  private void connect(CompletableFuture<ProtocolClientConnection> future) {
    if (!selector.hasNext()) {
      LOGGER.debug("Failed to connect to the cluster");
      future.complete(null);
    } else {
      Address address = selector.next();
      LOGGER.debug("Connecting to {}", address);
      client.connect(address).whenComplete((c, e) -> handleConnection(address, c, e, future));
    }
  }

  /**
   * Handles a connection to a server.
   */
  private void handleConnection(Address address, ProtocolClientConnection connection, Throwable error, CompletableFuture<ProtocolClientConnection> future) {
    if (open) {
      if (error == null) {
        setupConnection(address, connection, future);
      } else {
        connect(future);
      }
    }
  }

  /**
   * Sets up the given connection.
   */
  @SuppressWarnings("unchecked")
  private void setupConnection(Address address, ProtocolClientConnection connection, CompletableFuture<ProtocolClientConnection> future) {
    LOGGER.debug("Setting up connection to {}", address);

    this.connection = connection;
    connection.onClose(c -> {
      if (c.equals(this.connection)) {
        LOGGER.debug("Connection closed");
        this.connection = null;
      }
    });
    connection.onException(c -> {
      if (c.equals(this.connection)) {
        LOGGER.debug("Connection lost");
        this.connection = null;
      }
    });

    if (publishListener != null) {
      connection.onPublish(publishListener);
    }

    // When we first connect to a new server, first send a ConnectRequest to the server to establish
    // the connection with the server-side state machine.
    connection.connect(ConnectRequest.builder().withClient(id).build())
      .whenComplete((r, e) -> handleConnectResponse(r, e, future));
  }

  /**
   * Handles a connect response.
   */
  private void handleConnectResponse(ConnectResponse response, Throwable error, CompletableFuture<ProtocolClientConnection> future) {
    if (open) {
      if (error == null) {
        // If the connection was successfully created, immediately send a keep-alive request
        // to the server to ensure we maintain our session and get an updated list of server addresses.
        if (response.status() == ProtocolResponse.Status.OK) {
          selector.reset(new Address(response.leader()), response.members().stream().map(Address::new).collect(Collectors.toList()));
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
  public Listener<Throwable> onException(Consumer<Throwable> listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Listener<ProtocolConnection> onClose(Consumer<ProtocolConnection> listener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CompletableFuture<Void> close() {
    open = false;
    return CompletableFuture.completedFuture(null);
  }

}
