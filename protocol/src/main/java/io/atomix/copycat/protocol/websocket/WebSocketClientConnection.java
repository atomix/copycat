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
package io.atomix.copycat.protocol.websocket;

import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.atomix.copycat.protocol.ProtocolListener;
import io.atomix.copycat.protocol.ProtocolRequestFactory;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
import io.atomix.copycat.protocol.websocket.request.*;
import io.atomix.copycat.protocol.websocket.response.WebSocketPublishResponse;
import io.vertx.core.http.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Web socket client connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class WebSocketClientConnection extends AbstractWebSocketConnection<WebSocket> implements ProtocolClientConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketClientConnection.class);

  private ProtocolListener<PublishRequest, PublishResponse.Builder, PublishResponse> publishListener;
  private final AtomicLong id = new AtomicLong();

  public WebSocketClientConnection(WebSocket socket) {
    super(socket);
  }

  protected Logger logger() {
    return LOGGER;
  }

  @Override
  protected void onRequest(WebSocketRequest request) {
    switch (request.type()) {
      case PUBLISH_REQUEST:
        publishListener.onRequest((PublishRequest) request, new WebSocketPublishResponse.Builder(request.id()))
          .whenComplete((response, error) -> {
            if (error == null) {
              sendResponse((WebSocketPublishResponse) response);
            }
          });
        break;
    }
  }

  @Override
  public CompletableFuture<ConnectResponse> connect(ProtocolRequestFactory<ConnectRequest.Builder, ConnectRequest> builder) {
    return sendRequest((WebSocketConnectRequest) builder.build(new WebSocketConnectRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<RegisterResponse> register(ProtocolRequestFactory<RegisterRequest.Builder, RegisterRequest> builder) {
    return sendRequest((WebSocketRegisterRequest) builder.build(new WebSocketRegisterRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(ProtocolRequestFactory<KeepAliveRequest.Builder, KeepAliveRequest> builder) {
    return sendRequest((WebSocketKeepAliveRequest) builder.build(new WebSocketKeepAliveRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<UnregisterResponse> unregister(ProtocolRequestFactory<UnregisterRequest.Builder, UnregisterRequest> builder) {
    return sendRequest((WebSocketUnregisterRequest) builder.build(new WebSocketUnregisterRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<CommandResponse> command(ProtocolRequestFactory<CommandRequest.Builder, CommandRequest> builder) {
    return sendRequest((WebSocketCommandRequest) builder.build(new WebSocketCommandRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<QueryResponse> query(ProtocolRequestFactory<QueryRequest.Builder, QueryRequest> builder) {
    return sendRequest((WebSocketQueryRequest) builder.build(new WebSocketQueryRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public ProtocolClientConnection onPublish(ProtocolListener<PublishRequest, PublishResponse.Builder, PublishResponse> listener) {
    this.publishListener = listener;
    return this;
  }
}
