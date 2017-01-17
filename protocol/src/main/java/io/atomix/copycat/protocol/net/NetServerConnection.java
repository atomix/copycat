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
package io.atomix.copycat.protocol.net;

import io.atomix.copycat.protocol.ProtocolListener;
import io.atomix.copycat.protocol.ProtocolRequestFactory;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.protocol.net.request.NetPublishRequest;
import io.atomix.copycat.protocol.net.request.NetRequest;
import io.atomix.copycat.protocol.net.response.*;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
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
public class NetServerConnection extends NetConnection implements ProtocolServerConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(NetServerConnection.class);

  private ProtocolListener<ConnectRequest, ConnectResponse.Builder, ConnectResponse> connectListener;
  private ProtocolListener<RegisterRequest, RegisterResponse.Builder, RegisterResponse> registerListener;
  private ProtocolListener<KeepAliveRequest, KeepAliveResponse.Builder, KeepAliveResponse> keepAliveListener;
  private ProtocolListener<UnregisterRequest, UnregisterResponse.Builder, UnregisterResponse> unregisterListener;
  private ProtocolListener<CommandRequest, CommandResponse.Builder, CommandResponse> commandListener;
  private ProtocolListener<QueryRequest, QueryResponse.Builder, QueryResponse> queryListener;
  private final AtomicLong id = new AtomicLong();

  public NetServerConnection(NetSocket socket) {
    super(socket);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  protected boolean onRequest(NetRequest request) {
    if (request.type() == NetRequest.Types.CONNECT_REQUEST) {
      connectListener.onRequest((ConnectRequest) request, new NetConnectResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetConnectResponse) response);
          }
        });
    } else if (request.type() == NetRequest.Types.REGISTER_REQUEST) {
      registerListener.onRequest((RegisterRequest) request, new NetRegisterResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetRegisterResponse) response);
          }
        });
    } else if (request.type() == NetRequest.Types.KEEP_ALIVE_REQUEST) {
      keepAliveListener.onRequest((KeepAliveRequest) request, new NetKeepAliveResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetKeepAliveResponse) response);
          }
        });
    } else if (request.type() == NetRequest.Types.UNREGISTER_REQUEST) {
      unregisterListener.onRequest((UnregisterRequest) request, new NetUnregisterResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetUnregisterResponse) response);
          }
        });
    } else if (request.type() == NetRequest.Types.QUERY_REQUEST) {
      queryListener.onRequest((QueryRequest) request, new NetQueryResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetQueryResponse) response);
          }
        });
    } else if (request.type() == NetRequest.Types.COMMAND_REQUEST) {
      commandListener.onRequest((CommandRequest) request, new NetCommandResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetCommandResponse) response);
          }
        });
    } else {
      return false;
    }
    return true;
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
    return sendRequest((NetPublishRequest) builder.build(new NetPublishRequest.Builder(id.incrementAndGet())));
  }
}
