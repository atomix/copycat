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

import com.esotericsoftware.kryo.pool.KryoPool;
import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.atomix.copycat.protocol.ProtocolListener;
import io.atomix.copycat.protocol.ProtocolRequestFactory;
import io.atomix.copycat.protocol.net.request.*;
import io.atomix.copycat.protocol.net.response.NetPublishResponse;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
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
public class NetClientConnection extends NetConnection implements ProtocolClientConnection {
  private static final Logger LOGGER = LoggerFactory.getLogger(NetClientConnection.class);

  private ProtocolListener<PublishRequest, PublishResponse.Builder, PublishResponse> publishListener;
  protected final AtomicLong id = new AtomicLong();

  public NetClientConnection(NetSocket socket, KryoPool kryoPool) {
    super(socket, kryoPool);
  }

  @Override
  protected Logger logger() {
    return LOGGER;
  }

  @Override
  protected boolean onRequest(NetRequest request) {
    if (request.type() == NetRequest.Type.PUBLISH) {
      if (publishListener != null) {
        publishListener.onRequest((PublishRequest) request, new NetPublishResponse.Builder(request.id()))
          .whenComplete((response, error) -> {
            if (error == null) {
              sendResponse((NetPublishResponse) response);
            }
          });
      }
      return true;
    }
    return false;
  }

  @Override
  public CompletableFuture<ConnectResponse> connect(ProtocolRequestFactory<ConnectRequest.Builder, ConnectRequest> factory) {
    return sendRequest((NetConnectRequest) factory.build(new NetConnectRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<RegisterResponse> register(ProtocolRequestFactory<RegisterRequest.Builder, RegisterRequest> factory) {
    return sendRequest((NetRegisterRequest) factory.build(new NetRegisterRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(ProtocolRequestFactory<KeepAliveRequest.Builder, KeepAliveRequest> factory) {
    return sendRequest((NetKeepAliveRequest) factory.build(new NetKeepAliveRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<UnregisterResponse> unregister(ProtocolRequestFactory<UnregisterRequest.Builder, UnregisterRequest> factory) {
    return sendRequest((NetUnregisterRequest) factory.build(new NetUnregisterRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<CommandResponse> command(ProtocolRequestFactory<CommandRequest.Builder, CommandRequest> factory) {
    return sendRequest((NetCommandRequest) factory.build(new NetCommandRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<QueryResponse> query(ProtocolRequestFactory<QueryRequest.Builder, QueryRequest> factory) {
    return sendRequest((NetQueryRequest) factory.build(new NetQueryRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public ProtocolClientConnection onPublish(ProtocolListener<PublishRequest, PublishResponse.Builder, PublishResponse> listener) {
    this.publishListener = listener;
    return this;
  }
}
