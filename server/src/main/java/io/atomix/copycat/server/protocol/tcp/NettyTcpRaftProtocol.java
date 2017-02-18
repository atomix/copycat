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

import io.atomix.copycat.protocol.tcp.TcpOptions;
import io.atomix.copycat.server.protocol.RaftProtocol;
import io.atomix.copycat.server.protocol.RaftProtocolClient;
import io.atomix.copycat.server.protocol.RaftProtocolServer;
import io.atomix.copycat.util.concurrent.CopycatThreadFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

/**
 * Netty TCP based {@link RaftProtocol}.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NettyTcpRaftProtocol implements RaftProtocol {
  private final TcpOptions options;
  private final EventLoopGroup eventLoopGroup;

  public NettyTcpRaftProtocol() {
    this(TcpOptions.builder().build());
  }

  public NettyTcpRaftProtocol(TcpOptions options) {
    this.options = options;
    ThreadFactory threadFactory = new CopycatThreadFactory("copycat-event-loop-%d");
    eventLoopGroup = new NioEventLoopGroup(options.threads(), threadFactory);
  }

  @Override
  public RaftProtocolServer createServer() {
    return new NettyTcpRaftServer(eventLoopGroup, options);
  }

  @Override
  public RaftProtocolClient createClient() {
    return new NettyTcpRaftClient(eventLoopGroup, options);
  }

  @Override
  public CompletableFuture<Void> close() {
    try {
      eventLoopGroup.shutdownGracefully().sync();
    } catch (InterruptedException e) {
    }
    return CompletableFuture.completedFuture(null);
  }
}
