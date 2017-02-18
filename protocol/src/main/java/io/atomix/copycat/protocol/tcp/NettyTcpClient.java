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

import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.ProtocolClient;
import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Netty protocol client.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NettyTcpClient extends NettyTcpClientBase<NettyTcpClientConnection> implements ProtocolClient {
  private final Map<Channel, NettyTcpClientConnection> connections = new ConcurrentHashMap<>();

  public NettyTcpClient(EventLoopGroup eventLoopGroup, TcpOptions options) {
    super(eventLoopGroup, options);
  }

  protected NettyTcpHandler createHandler(Consumer<NettyTcpClientConnection> callback) {
    return new NettyTcpHandler<>(c -> new NettyTcpClientConnection(c, options), connections, callback);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<ProtocolClientConnection> connect(Address address) {
    return (CompletableFuture) bootstrap(address);
  }

  @Override
  public CompletableFuture<Void> close() {
    int i = 0;
    CompletableFuture<?>[] futures = new CompletableFuture[connections.size()];
    for (NettyTcpClientConnection connection : connections.values()) {
      futures[i++] = connection.close();
    }
    return CompletableFuture.allOf(futures);
  }
}
