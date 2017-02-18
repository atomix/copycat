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
import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.atomix.copycat.util.Assert;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Netty protocol client.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class NettyTcpClientBase<T extends ProtocolClientConnection> {
  private static final Logger LOGGER = LoggerFactory.getLogger(NettyTcpClientBase.class);
  private static final ByteBufAllocator ALLOCATOR = new PooledByteBufAllocator(true);
  private static final ChannelHandler FIELD_PREPENDER = new LengthFieldPrepender(4);

  protected final EventLoopGroup eventLoopGroup;
  protected final TcpOptions options;

  protected NettyTcpClientBase(EventLoopGroup eventLoopGroup, TcpOptions options) {
    this.eventLoopGroup = Assert.notNull(eventLoopGroup, "eventLoopGroup");
    this.options = Assert.notNull(options, "options");
  }

  protected abstract NettyTcpHandler createHandler(Consumer<T> callback);

  protected CompletableFuture<T> bootstrap(Address address) {
    Assert.notNull(address, "address");
    CompletableFuture<T> future = new CompletableFuture<>();

    LOGGER.info("Connecting to {}", address);

    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(eventLoopGroup)
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
          ChannelPipeline pipeline = channel.pipeline();
          if (options.ssl().enabled()) {
            pipeline.addFirst(new SslHandler(new NettyTcpTls(options.ssl()).initSslEngine(true)));
          }
          pipeline.addLast(FIELD_PREPENDER);
          pipeline.addLast(new LengthFieldBasedFrameDecoder(options.maxFrameSize(), 0, 4, 0, 4));
          pipeline.addLast(createHandler(future::complete));
        }
      });

    bootstrap.option(ChannelOption.TCP_NODELAY, options.tcpNoDelay());
    bootstrap.option(ChannelOption.SO_KEEPALIVE, options.tcpKeepAlive());
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, options.connectTimeout());
    bootstrap.option(ChannelOption.ALLOCATOR, ALLOCATOR);

    if (options.sendBufferSize() != -1) {
      bootstrap.option(ChannelOption.SO_SNDBUF, options.sendBufferSize());
    }
    if (options.receiveBufferSize() != -1) {
      bootstrap.option(ChannelOption.SO_RCVBUF, options.receiveBufferSize());
    }

    bootstrap.connect(new InetSocketAddress(address.host(), address.port())).addListener(channelFuture -> {
      if (channelFuture.isSuccess()) {
        LOGGER.info("Connected to {}", address);
      } else {
        future.completeExceptionally(channelFuture.cause());
      }
    });
    return future;
  }
}
