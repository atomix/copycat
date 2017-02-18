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

import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.atomix.copycat.protocol.ProtocolListener;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Netty TCP client connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NettyTcpClientConnection extends NettyTcpConnection implements ProtocolClientConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyTcpClientConnection.class);

  private ProtocolListener<PublishRequest, PublishResponse> publishListener;

  public NettyTcpClientConnection(io.netty.channel.Channel channel, TcpOptions options) {
    super(channel, options);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  protected boolean onRequest(long id, ProtocolRequest request) {
    if (request.type() == ProtocolRequest.Type.PUBLISH) {
      onRequest(id, (PublishRequest) request, publishListener);
      return true;
    }
    return false;
  }

  @Override
  public CompletableFuture<ConnectResponse> connect(ConnectRequest request) {
    return sendRequest(request);
  }

  @Override
  public CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    return sendRequest(request);
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    return sendRequest(request);
  }

  @Override
  public CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request) {
    return sendRequest(request);
  }

  @Override
  public CompletableFuture<CommandResponse> command(CommandRequest request) {
    return sendRequest(request);
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    return sendRequest(request);
  }

  @Override
  public ProtocolClientConnection onPublish(ProtocolListener<PublishRequest, PublishResponse> listener) {
    this.publishListener = listener;
    return this;
  }
}
