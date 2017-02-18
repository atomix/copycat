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
 * Protocol client connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ProtocolClientConnection extends ProtocolConnection {

  /**
   * Sends a connect request to the server.
   *
   * @param request The connect builder.
   * @return A completable future to be completed once the connect response is received.
   */
  CompletableFuture<ConnectResponse> connect(ConnectRequest request);

  /**
   * Sends a register request to the server.
   *
   * @param request The register builder.
   * @return A completable future to be completed once the register response is received.
   */
  CompletableFuture<RegisterResponse> register(RegisterRequest request);

  /**
   * Sends a keep-alive request to the server.
   *
   * @param request The keep-alive builder.
   * @return A completable future to be completed once the keep-alive response is received.
   */
  CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request);

  /**
   * Sends an unregister request to the server.
   *
   * @param request The unregister builder.
   * @return A completable future to be completed once the unregister response is received.
   */
  CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request);

  /**
   * Sends a command request to the server.
   *
   * @param request The command builder.
   * @return A completable future to be completed once the command response is received.
   */
  CompletableFuture<CommandResponse> command(CommandRequest request);

  /**
   * Sends a query request to the server.
   *
   * @param request The query builder.
   * @return A completable future to be completed once the query response is received.
   */
  CompletableFuture<QueryResponse> query(QueryRequest request);

  /**
   * Registers a listener to be called when a publish request is received from the server.
   *
   * @param listener The listener to be called when a publish request is received from the server.
   * @return The client connection.
   */
  ProtocolClientConnection onPublish(ProtocolListener<PublishRequest, PublishResponse> listener);

}
