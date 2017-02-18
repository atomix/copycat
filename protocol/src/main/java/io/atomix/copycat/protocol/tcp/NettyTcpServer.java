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
import io.atomix.copycat.protocol.ProtocolServer;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.EventLoopGroup;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Netty protocol server.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NettyTcpServer extends NettyTcpServerBase<NettyTcpServerConnection> implements ProtocolServer {
  private final Map<Channel, NettyTcpServerConnection> connections = new ConcurrentHashMap<>();

  public NettyTcpServer(EventLoopGroup eventLoopGroup, TcpOptions options) {
    super(eventLoopGroup, options);
  }

  @Override
  protected NettyTcpHandler createHandler(Consumer<NettyTcpServerConnection> callback) {
    return new ServerHandler(c -> new NettyTcpServerConnection(c, options), connections, callback);
  }

  @Override
  @SuppressWarnings("unchecked")
  public CompletableFuture<Void> listen(Address address, Consumer<ProtocolServerConnection> listener) {
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
  private static class ServerHandler extends NettyTcpHandler<NettyTcpServerConnection> {
    private ServerHandler(Function<Channel, NettyTcpServerConnection> factory, Map<Channel, NettyTcpServerConnection> connections, Consumer<NettyTcpServerConnection> listener) {
      super(factory, connections, listener);
    }
  }

}
