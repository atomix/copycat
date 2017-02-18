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
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Netty TCP server connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NettyTcpServerConnection extends NettyTcpConnection implements ProtocolServerConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyTcpServerConnection.class);

  private ProtocolListener<ConnectRequest, ConnectResponse> connectListener;
  private ProtocolListener<RegisterRequest, RegisterResponse> registerListener;
  private ProtocolListener<KeepAliveRequest, KeepAliveResponse> keepAliveListener;
  private ProtocolListener<UnregisterRequest, UnregisterResponse> unregisterListener;
  private ProtocolListener<CommandRequest, CommandResponse> commandListener;
  private ProtocolListener<QueryRequest, QueryResponse> queryListener;

  public NettyTcpServerConnection(Channel channel, TcpOptions options) {
    super(channel, options);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  protected boolean onRequest(long id, ProtocolRequest request) {
    if (request.type() == ProtocolRequest.Type.CONNECT) {
      connectListener.onRequest((ConnectRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else if (request.type() == ProtocolRequest.Type.REGISTER) {
      registerListener.onRequest((RegisterRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else if (request.type() == ProtocolRequest.Type.KEEP_ALIVE) {
      keepAliveListener.onRequest((KeepAliveRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else if (request.type() == ProtocolRequest.Type.UNREGISTER) {
      unregisterListener.onRequest((UnregisterRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else if (request.type() == ProtocolRequest.Type.QUERY) {
      queryListener.onRequest((QueryRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else if (request.type() == ProtocolRequest.Type.COMMAND) {
      commandListener.onRequest((CommandRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else {
      return false;
    }
    return true;
  }

  @Override
  public ProtocolServerConnection onConnect(ProtocolListener<ConnectRequest, ConnectResponse> listener) {
    this.connectListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onRegister(ProtocolListener<RegisterRequest, RegisterResponse> listener) {
    this.registerListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onKeepAlive(ProtocolListener<KeepAliveRequest, KeepAliveResponse> listener) {
    this.keepAliveListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onUnregister(ProtocolListener<UnregisterRequest, UnregisterResponse> listener) {
    this.unregisterListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onCommand(ProtocolListener<CommandRequest, CommandResponse> listener) {
    this.commandListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onQuery(ProtocolListener<QueryRequest, QueryResponse> listener) {
    this.queryListener = listener;
    return this;
  }

  @Override
  public CompletableFuture<PublishResponse> publish(PublishRequest request) {
    return sendRequest(request);
  }
}
