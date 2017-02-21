/*
 * Copyright 2017 the original author or authors.
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
package io.atomix.copycat.server.protocol.tcp;

import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.tcp.NettyTcpClientBase;
import io.atomix.copycat.protocol.tcp.NettyTcpClientConnection;
import io.atomix.copycat.protocol.tcp.NettyTcpHandler;
import io.atomix.copycat.protocol.tcp.TcpOptions;
import io.atomix.copycat.server.protocol.RaftProtocolClient;
import io.atomix.copycat.server.protocol.RaftProtocolClientConnection;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Netty TCP based Raft client.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NettyTcpRaftClient extends NettyTcpClientBase<NettyTcpRaftClientConnection> implements RaftProtocolClient {
  private final Map<Channel, NettyTcpRaftClientConnection> connections = new ConcurrentHashMap<>();

  public NettyTcpRaftClient(EventLoopGroup eventLoopGroup, TcpOptions options) {
    super(eventLoopGroup, options);
  }

  @Override
  protected NettyTcpHandler createHandler(Consumer<NettyTcpRaftClientConnection> callback) {
    return new NettyTcpHandler<>(c -> new NettyTcpRaftClientConnection(c, options), connections, callback);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<RaftProtocolClientConnection> connect(Address address) {
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
