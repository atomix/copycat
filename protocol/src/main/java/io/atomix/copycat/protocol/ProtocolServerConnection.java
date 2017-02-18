/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.copycat.protocol;

import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;

import java.util.concurrent.CompletableFuture;

/**
 * Protocol server connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ProtocolServerConnection extends ProtocolConnection {

  /**
   * Registers a listener to be called when a connect request is received from the server.
   *
   * @param listener The listener to be called when a connect request is received from the server.
   * @return The client connection.
   */
  ProtocolServerConnection onConnect(ProtocolListener<ConnectRequest, ConnectResponse> listener);

  /**
   * Registers a listener to be called when a register request is received from the server.
   *
   * @param listener The listener to be called when a register request is received from the server.
   * @return The client connection.
   */
  ProtocolServerConnection onRegister(ProtocolListener<RegisterRequest, RegisterResponse> listener);

  /**
   * Registers a listener to be called when a keep-alive request is received from the server.
   *
   * @param listener The listener to be called when a keep-alive request is received from the server.
   * @return The client connection.
   */
  ProtocolServerConnection onKeepAlive(ProtocolListener<KeepAliveRequest, KeepAliveResponse> listener);

  /**
   * Registers a listener to be called when an unregister request is received from the server.
   *
   * @param listener The listener to be called when an unregister request is received from the server.
   * @return The client connection.
   */
  ProtocolServerConnection onUnregister(ProtocolListener<UnregisterRequest, UnregisterResponse> listener);

  /**
   * Registers a listener to be called when a command request is received from the server.
   *
   * @param listener The listener to be called when a command request is received from the server.
   * @return The client connection.
   */
  ProtocolServerConnection onCommand(ProtocolListener<CommandRequest, CommandResponse> listener);

  /**
   * Registers a listener to be called when a query request is received from the server.
   *
   * @param listener The listener to be called when a query request is received from the server.
   * @return The client connection.
   */
  ProtocolServerConnection onQuery(ProtocolListener<QueryRequest, QueryResponse> listener);

  /**
   * Sends a publish request to the client.
   *
   * @param request The publish request.
   * @return A completable future to be completed once the publish response is received.
   */
  CompletableFuture<PublishResponse> publish(PublishRequest request);

}
