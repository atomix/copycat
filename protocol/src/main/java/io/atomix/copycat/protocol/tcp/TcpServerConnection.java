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
package io.atomix.copycat.protocol.tcp;

import io.atomix.copycat.protocol.ProtocolListener;
import io.atomix.copycat.protocol.ProtocolRequestFactory;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
import io.atomix.copycat.protocol.tcp.request.NetSocketPublishRequest;
import io.atomix.copycat.protocol.tcp.request.NetSocketRequest;
import io.atomix.copycat.protocol.tcp.response.*;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TCP server connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class TcpServerConnection extends TcpConnection implements ProtocolServerConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(TcpServerConnection.class);

  private ProtocolListener<ConnectRequest, ConnectResponse.Builder, ConnectResponse> connectListener;
  private ProtocolListener<RegisterRequest, RegisterResponse.Builder, RegisterResponse> registerListener;
  private ProtocolListener<KeepAliveRequest, KeepAliveResponse.Builder, KeepAliveResponse> keepAliveListener;
  private ProtocolListener<UnregisterRequest, UnregisterResponse.Builder, UnregisterResponse> unregisterListener;
  private ProtocolListener<CommandRequest, CommandResponse.Builder, CommandResponse> commandListener;
  private ProtocolListener<QueryRequest, QueryResponse.Builder, QueryResponse> queryListener;
  private final AtomicLong id = new AtomicLong();

  public TcpServerConnection(NetSocket socket) {
    super(socket);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  protected void onRequest(NetSocketRequest request) {
    switch (request.type()) {
      case CONNECT_REQUEST:
        connectListener.onRequest((ConnectRequest) request, new NetSocketConnectResponse.Builder(request.id()))
          .whenComplete((response, error) -> {
            if (error == null) {
              sendResponse((NetSocketConnectResponse) response);
            }
          });
        break;
      case REGISTER_REQUEST:
        registerListener.onRequest((RegisterRequest) request, new NetSocketRegisterResponse.Builder(request.id()))
          .whenComplete((response, error) -> {
            if (error == null) {
              sendResponse((NetSocketRegisterResponse) response);
            }
          });
        break;
      case KEEP_ALIVE_REQUEST:
        keepAliveListener.onRequest((KeepAliveRequest) request, new NetSocketKeepAliveResponse.Builder(request.id()))
          .whenComplete((response, error) -> {
            if (error == null) {
              sendResponse((NetSocketKeepAliveResponse) response);
            }
          });
        break;
      case UNREGISTER_REQUEST:
        unregisterListener.onRequest((UnregisterRequest) request, new NetSocketUnregisterResponse.Builder(request.id()))
          .whenComplete((response, error) -> {
            if (error == null) {
              sendResponse((NetSocketUnregisterResponse) response);
            }
          });
        break;
      case COMMAND_REQUEST:
        commandListener.onRequest((CommandRequest) request, new NetSocketCommandResponse.Builder(request.id()))
          .whenComplete((response, error) -> {
            if (error == null) {
              sendResponse((NetSocketCommandResponse) response);
            }
          });
        break;
      case QUERY_REQUEST:
        queryListener.onRequest((QueryRequest) request, new NetSocketQueryResponse.Builder(request.id()))
          .whenComplete((response, error) -> {
            if (error == null) {
              sendResponse((NetSocketQueryResponse) response);
            }
          });
        break;
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
    return sendRequest((NetSocketPublishRequest) builder.build(new NetSocketPublishRequest.Builder(id.incrementAndGet())));
  }
}
