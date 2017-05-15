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
import io.atomix.copycat.client.util.AddressSelector;
import io.atomix.copycat.client.util.ClientConnectionManager;
import io.atomix.copycat.protocol.ConnectRequest;
import io.atomix.copycat.protocol.ConnectResponse;
import io.atomix.copycat.protocol.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Session connection.
 */
public class CopycatSessionConnection extends CopycatConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(CopycatSessionConnection.class);
  private final CopycatSessionState state;
  private final String sessionString;

  public CopycatSessionConnection(CopycatSessionState state, ClientConnectionManager connections, AddressSelector selector) {
    super(connections, selector);
    this.state = state;
    this.sessionString = String.valueOf(state.getSessionId());
  }

  @Override
  protected String name() {
    return sessionString;
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
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

    // When we first connect to a new server, first send a ConnectRequest to the server to establish
    // the connection with the server-side state machine.
    ConnectRequest request = ConnectRequest.builder()
      .withSession(state.getSessionId())
      .withConnection(state.nextConnection())
      .build();

    logger().trace("{} - Sending {}", name(), request);
    connection.<ConnectRequest, ConnectResponse>sendAndReceive(ConnectRequest.NAME, request).whenComplete((r, e) -> handleConnectResponse(r, e, future));
  }

  /**
   * Handles a connect response.
   */
  private void handleConnectResponse(ConnectResponse response, Throwable error, CompletableFuture<Connection> future) {
    if (open) {
      if (error == null) {
        logger().trace("{} - Received {}", name(), response);
        // If the connection was successfully created, immediately send a keep-alive request
        // to the server to ensure we maintain our session and get an updated list of server addresses.
        if (response.status() == Response.Status.OK) {
          selector.reset(response.leader(), response.members());
          future.complete(connection);
        } else {
          connect(future);
        }
      } else {
        logger().debug("{} - Failed to connect! Reason: {}", name(), error);
        connect(future);
      }
    }
  }

}
