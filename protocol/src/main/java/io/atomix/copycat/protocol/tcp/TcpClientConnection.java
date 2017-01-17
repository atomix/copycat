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
import io.atomix.copycat.protocol.ProtocolRequestFactory;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
import io.atomix.copycat.protocol.tcp.request.*;
import io.atomix.copycat.protocol.tcp.response.NetSocketPublishResponse;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TCP client connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class TcpClientConnection extends TcpConnection implements ProtocolClientConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(TcpClientConnection.class);

  private ProtocolListener<PublishRequest, PublishResponse.Builder, PublishResponse> publishListener;
  private final AtomicLong id = new AtomicLong();

  public TcpClientConnection(NetSocket socket) {
    super(socket);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  protected void onRequest(NetSocketRequest request) {
    switch (request.type()) {
      case PUBLISH_REQUEST:
        if (publishListener != null) {
          publishListener.onRequest((PublishRequest) request, new NetSocketPublishResponse.Builder(request.id()))
            .whenComplete((response, error) -> {
              if (error == null) {
                sendResponse((NetSocketPublishResponse) response);
              }
            });
        }
        break;
    }
  }

  @Override
  public CompletableFuture<ConnectResponse> connect(ProtocolRequestFactory<ConnectRequest.Builder, ConnectRequest> factory) {
    return sendRequest((NetSocketConnectRequest) factory.build(new NetSocketConnectRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<RegisterResponse> register(ProtocolRequestFactory<RegisterRequest.Builder, RegisterRequest> factory) {
    return sendRequest((NetSocketRegisterRequest) factory.build(new NetSocketRegisterRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(ProtocolRequestFactory<KeepAliveRequest.Builder, KeepAliveRequest> factory) {
    return sendRequest((NetSocketKeepAliveRequest) factory.build(new NetSocketKeepAliveRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<UnregisterResponse> unregister(ProtocolRequestFactory<UnregisterRequest.Builder, UnregisterRequest> factory) {
    return sendRequest((NetSocketUnregisterRequest) factory.build(new NetSocketUnregisterRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<CommandResponse> command(ProtocolRequestFactory<CommandRequest.Builder, CommandRequest> factory) {
    return sendRequest((NetSocketCommandRequest) factory.build(new NetSocketCommandRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<QueryResponse> query(ProtocolRequestFactory<QueryRequest.Builder, QueryRequest> factory) {
    return sendRequest((NetSocketQueryRequest) factory.build(new NetSocketQueryRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public ProtocolClientConnection onPublish(ProtocolListener<PublishRequest, PublishResponse.Builder, PublishResponse> listener) {
    this.publishListener = listener;
    return this;
  }
}
