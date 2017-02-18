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
package io.atomix.copycat.protocol.tcp;

import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.util.Assert;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Base class for Netty TCP servers.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class NettyTcpServerBase<T extends ProtocolServerConnection> {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyTcpServer.class);
  private static final ByteBufAllocator ALLOCATOR = new PooledByteBufAllocator(true);
  private static final ChannelHandler FIELD_PREPENDER = new LengthFieldPrepender(4);

  protected final EventLoopGroup eventLoopGroup;
  protected final TcpOptions options;
  protected NettyTcpHandler handler;
  protected ChannelGroup channelGroup;
  private final Object listenLock = new Object();
  private volatile boolean listening;
  private CompletableFuture<Void> listenFuture;

  protected NettyTcpServerBase(EventLoopGroup eventLoopGroup, TcpOptions options) {
    this.eventLoopGroup = Assert.notNull(eventLoopGroup, "eventLoopGroup");
    this.options = Assert.notNull(options, "options");
  }

  protected abstract NettyTcpHandler createHandler(Consumer<T> callback);

  public CompletableFuture<Void> bootstrap(Address address, Consumer<T> listener) {
    Assert.notNull(address, "address");
    Assert.notNull(listener, "listener");
    if (listening)
      return CompletableFuture.completedFuture(null);

    synchronized (listenLock) {
      if (listenFuture == null) {
        listenFuture = new CompletableFuture<>();
        bind(address, listener);
      }
    }
    return listenFuture;
  }

  /**
   * Starts listening for the given member.
   */
  @SuppressWarnings("unchecked")
  protected void bind(Address address, Consumer listener) {
    channelGroup = new DefaultChannelGroup("catalyst-acceptor-channels", GlobalEventExecutor.INSTANCE);

    handler = createHandler(listener);

    final ServerBootstrap bootstrap = new ServerBootstrap();
    bootstrap.group(eventLoopGroup)
      .channel(NioServerSocketChannel.class)
      .handler(new LoggingHandler(LogLevel.DEBUG))
      .childHandler(new ChannelInitializer<SocketChannel>() {
        @Override
        public void initChannel(SocketChannel channel) throws Exception {
          ChannelPipeline pipeline = channel.pipeline();
          if (options.ssl().enabled()) {
            pipeline.addFirst(new SslHandler(new NettyTcpTls(options.ssl()).initSslEngine(false)));
          }
          pipeline.addLast(FIELD_PREPENDER);
          pipeline.addLast(new LengthFieldBasedFrameDecoder(options.maxFrameSize(), 0, 4, 0, 4));
          pipeline.addLast(handler);
        }
      })
      .option(ChannelOption.SO_BACKLOG, options.acceptBacklog())
      .option(ChannelOption.TCP_NODELAY, options.tcpNoDelay())
      .option(ChannelOption.SO_REUSEADDR, options.reuseAddress())
      .childOption(ChannelOption.ALLOCATOR, ALLOCATOR)
      .childOption(ChannelOption.SO_KEEPALIVE, options.tcpKeepAlive());

    if (options.sendBufferSize() != -1) {
      bootstrap.childOption(ChannelOption.SO_SNDBUF, options.sendBufferSize());
    }
    if (options.receiveBufferSize() != -1) {
      bootstrap.childOption(ChannelOption.SO_RCVBUF, options.receiveBufferSize());
    }

    LOGGER.info("Binding to {}", address);

    ChannelFuture bindFuture = bootstrap.bind(new InetSocketAddress(address.host(), address.port()));
    bindFuture.addListener(channelFuture -> {
      if (channelFuture.isSuccess()) {
        listening = true;
        LOGGER.info("Listening at {}", bindFuture.channel().localAddress());
        listenFuture.complete(null);
      } else {
        listenFuture.completeExceptionally(channelFuture.cause());
      }
    });
    channelGroup.add(bindFuture.channel());
  }

}
