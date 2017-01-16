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
   * @param builder The connect request builder.
   * @return A completable future to be completed once the connect response is received.
   */
  CompletableFuture<ConnectResponse> connect(ProtocolRequestFactory<ConnectRequest.Builder, ConnectRequest> builder);

  /**
   * Sends a register request to the server.
   *
   * @param builder The register request builder.
   * @return A completable future to be completed once the register response is received.
   */
  CompletableFuture<RegisterResponse> register(ProtocolRequestFactory<RegisterRequest.Builder, RegisterRequest> builder);

  /**
   * Sends a keep-alive request to the server.
   *
   * @param builder The keep-alive request builder.
   * @return A completable future to be completed once the keep-alive response is received.
   */
  CompletableFuture<KeepAliveResponse> keepAlive(ProtocolRequestFactory<KeepAliveRequest.Builder, KeepAliveRequest> builder);

  /**
   * Sends an unregister request to the server.
   *
   * @param builder The unregister request builder.
   * @return A completable future to be completed once the unregister response is received.
   */
  CompletableFuture<UnregisterResponse> unregister(ProtocolRequestFactory<UnregisterRequest.Builder, UnregisterRequest> builder);

  /**
   * Sends a command request to the server.
   *
   * @param builder The command request builder.
   * @return A completable future to be completed once the command response is received.
   */
  CompletableFuture<CommandResponse> command(ProtocolRequestFactory<CommandRequest.Builder, CommandRequest> builder);

  /**
   * Sends a query request to the server.
   *
   * @param builder The query request builder.
   * @return A completable future to be completed once the query response is received.
   */
  CompletableFuture<QueryResponse> query(ProtocolRequestFactory<QueryRequest.Builder, QueryRequest> builder);

  /**
   * Registers a listener to be called when a publish request is received from the server.
   *
   * @param listener The listener to be called when a publish request is received from the server.
   * @return The client connection.
   */
  ProtocolClientConnection onPublish(ProtocolListener<PublishRequest, PublishResponse.Builder, PublishResponse> listener);

}
