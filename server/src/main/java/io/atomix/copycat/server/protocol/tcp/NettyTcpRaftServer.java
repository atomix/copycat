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
import io.atomix.copycat.protocol.tcp.NettyTcpHandler;
import io.atomix.copycat.protocol.tcp.NettyTcpServerBase;
import io.atomix.copycat.protocol.tcp.NettyTcpServerConnection;
import io.atomix.copycat.protocol.tcp.TcpOptions;
import io.atomix.copycat.server.protocol.RaftProtocolServer;
import io.atomix.copycat.server.protocol.RaftProtocolServerConnection;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Netty TCP based Raft server.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NettyTcpRaftServer extends NettyTcpServerBase<NettyTcpRaftServerConnection> implements RaftProtocolServer {
  private final Map<Channel, NettyTcpRaftServerConnection> connections = new ConcurrentHashMap<>();

  public NettyTcpRaftServer(EventLoopGroup eventLoopGroup, TcpOptions options) {
    super(eventLoopGroup, options);
  }

  @Override
  protected NettyTcpHandler createHandler(Consumer<NettyTcpRaftServerConnection> callback) {
    return new ServerHandler(c -> new NettyTcpRaftServerConnection(c, options), connections, callback);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> listen(Address address, Consumer<RaftProtocolServerConnection> listener) {
    return bootstrap(address, (Consumer) listener);
  }

  @Override
  public CompletableFuture<Void> close() {
    int i = 0;
    CompletableFuture<?>[] futures = new CompletableFuture[connections.size()];
    for (NettyTcpServerConnection connection : connections.values()) {
      futures[i++] = connection.close();
    }

    CompletableFuture<Void> future = new CompletableFuture<>();
    CompletableFuture.allOf(futures).whenComplete((result, error) -> {
      channelGroup.close().addListener(channelFuture -> {
        future.complete(null);
      });
    });
    return future;
  }

  /**
   * Server handler.
   */
  @ChannelHandler.Sharable
  private static class ServerHandler extends NettyTcpHandler<NettyTcpRaftServerConnection> {
    private ServerHandler(Function<Channel, NettyTcpRaftServerConnection> factory, Map<Channel, NettyTcpRaftServerConnection> connections, Consumer<NettyTcpRaftServerConnection> listener) {
      super(factory, connections, listener);
    }
  }

}
