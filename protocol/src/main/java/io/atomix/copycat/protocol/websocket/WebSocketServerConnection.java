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

import io.atomix.copycat.protocol.ProtocolListener;
import io.atomix.copycat.protocol.ProtocolRequestFactory;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
import io.atomix.copycat.protocol.websocket.request.WebSocketPublishRequest;
import io.atomix.copycat.protocol.websocket.request.WebSocketRequest;
import io.atomix.copycat.protocol.websocket.response.*;
import io.vertx.core.http.ServerWebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Web socket server connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class WebSocketServerConnection extends AbstractWebSocketConnection<ServerWebSocket> implements ProtocolServerConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(WebSocketServerConnection.class);

  private ProtocolListener<ConnectRequest, ConnectResponse.Builder, ConnectResponse> connectListener;
  private ProtocolListener<RegisterRequest, RegisterResponse.Builder, RegisterResponse> registerListener;
  private ProtocolListener<KeepAliveRequest, KeepAliveResponse.Builder, KeepAliveResponse> keepAliveListener;
  private ProtocolListener<UnregisterRequest, UnregisterResponse.Builder, UnregisterResponse> unregisterListener;
  private ProtocolListener<CommandRequest, CommandResponse.Builder, CommandResponse> commandListener;
  private ProtocolListener<QueryRequest, QueryResponse.Builder, QueryResponse> queryListener;
  private final AtomicLong id = new AtomicLong();

  public WebSocketServerConnection(ServerWebSocket socket) {
    super(socket);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  protected void onRequest(WebSocketRequest request) {
    if (request.type() == WebSocketRequest.Types.CONNECT_REQUEST) {
      connectListener.onRequest((ConnectRequest) request, new WebSocketConnectResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((WebSocketConnectResponse) response);
          }
        });
    } else if (request.type() == WebSocketRequest.Types.REGISTER_REQUEST) {
      registerListener.onRequest((RegisterRequest) request, new WebSocketRegisterResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((WebSocketRegisterResponse) response);
          }
        });
    } else if (request.type() == WebSocketRequest.Types.KEEP_ALIVE_REQUEST) {
      keepAliveListener.onRequest((KeepAliveRequest) request, new WebSocketKeepAliveResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((WebSocketKeepAliveResponse) response);
          }
        });
    } else if (request.type() == WebSocketRequest.Types.UNREGISTER_REQUEST) {
      unregisterListener.onRequest((UnregisterRequest) request, new WebSocketUnregisterResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((WebSocketUnregisterResponse) response);
          }
        });
    } else if (request.type() == WebSocketRequest.Types.COMMAND_REQUEST) {
      commandListener.onRequest((CommandRequest) request, new WebSocketCommandResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((WebSocketCommandResponse) response);
          }
        });
    } else if (request.type() == WebSocketRequest.Types.QUERY_REQUEST) {
      queryListener.onRequest((QueryRequest) request, new WebSocketQueryResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((WebSocketQueryResponse) response);
          }
        });
    }
  }

  @Override
  public ProtocolServerConnection onConnect(ProtocolListener<ConnectRequest, ConnectResponse.Builder, ConnectResponse> listener) {
    this.connectListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onRegister(ProtocolListener<RegisterRequest, RegisterResponse.Builder, RegisterResponse> listener) {
    this.registerListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onKeepAlive(ProtocolListener<KeepAliveRequest, KeepAliveResponse.Builder, KeepAliveResponse> listener) {
    this.keepAliveListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onUnregister(ProtocolListener<UnregisterRequest, UnregisterResponse.Builder, UnregisterResponse> listener) {
    this.unregisterListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onCommand(ProtocolListener<CommandRequest, CommandResponse.Builder, CommandResponse> listener) {
    this.commandListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onQuery(ProtocolListener<QueryRequest, QueryResponse.Builder, QueryResponse> listener) {
    this.queryListener = listener;
    return this;
  }

  @Override
  public CompletableFuture<PublishResponse> publish(ProtocolRequestFactory<PublishRequest.Builder, PublishRequest> builder) {
    return sendRequest((WebSocketPublishRequest) builder.build(new WebSocketPublishRequest.Builder(id.incrementAndGet())));
  }
}
