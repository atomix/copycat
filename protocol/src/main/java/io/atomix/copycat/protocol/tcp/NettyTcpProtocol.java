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

import io.atomix.copycat.protocol.Protocol;
import io.atomix.copycat.protocol.ProtocolClient;
import io.atomix.copycat.protocol.ProtocolServer;
import io.atomix.copycat.protocol.ssl.SslOptions;
import io.atomix.copycat.util.concurrent.CopycatThreadFactory;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;

/**
 * Netty TCP protocol implementation.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NettyTcpProtocol implements Protocol {
  private final TcpOptions options;
  private final EventLoopGroup eventLoopGroup;

  public NettyTcpProtocol() {
    this(TcpOptions.builder().build());
  }

  public NettyTcpProtocol(TcpOptions options) {
    this.options = options;
    ThreadFactory threadFactory = new CopycatThreadFactory("copycat-event-loop-%d");
    eventLoopGroup = new NioEventLoopGroup(options.threads(), threadFactory);
  }

  @Override
  public ProtocolClient createClient() {
    return new NettyTcpClient(eventLoopGroup, options);
  }

  @Override
  public ProtocolServer createServer() {
    return new NettyTcpServer(eventLoopGroup, options);
  }

  @Override
  public CompletableFuture<Void> close() {
    try {
      eventLoopGroup.shutdownGracefully().sync();
    } catch (InterruptedException e) {
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Netty transport builder.
   */
  public static class Builder implements Protocol.Builder<NettyTcpProtocol> {
    private final TcpOptions.Builder builder = TcpOptions.builder();

    private Builder() {
    }

    /**
     * Sets the number of Netty event loop threads.
     *
     * @param threads The number of Netty event loop threads.
     * @return The Netty transport builder.
     */
    public Builder withThreads(int threads) {
      builder.withThreads(threads);
      return this;
    }

    /**
     * Sets the Netty connect timeout.
     *
     * @param timeout The connect timeout.
     * @return The Netty transport builder.
     */
    public Builder withConnectTimeout(int timeout) {
      builder.withConnectTimeout(timeout);
      return this;
    }

    /**
     * Sets the send buffer size.
     *
     * @param sendBufferSize The send buffer size.
     * @return The Netty transport builder.
     */
    public Builder withSendBufferSize(int sendBufferSize) {
      builder.withSendBufferSize(sendBufferSize);
      return this;
    }

    /**
     * Sets the receive buffer size.
     *
     * @param receiveBufferSize The receive buffer size.
     * @return The Netty transport builder.
     */
    public Builder withReceiveBufferSize(int receiveBufferSize) {
      builder.withReceiveBufferSize(receiveBufferSize);
      return this;
    }

    /**
     * Sets the maximum frame size.
     *
     * @param maxFrameSize The maximum frame size.
     * @return The Netty transport builder.
     */
    public Builder withMaxFrameSize(int maxFrameSize) {
      builder.withMaxFrameSize(maxFrameSize);
      return this;
    }

    /**
     * Enables the SO_REUSEADDR option.
     *
     * @return The Netty transport builder.
     */
    public Builder withReuseAddress() {
      return withReuseAddress(true);
    }

    /**
     * Sets the SO_REUSEADDR option.
     *
     * @param reuseAddress Whether to enable SO_REUSEADDR.
     * @return The Netty transport builder.
     */
    public Builder withReuseAddress(boolean reuseAddress) {
      builder.withReuseAddress(reuseAddress);
      return this;
    }

    /**
     * Enables the SO_KEEPALIVE option.
     *
     * @return The Netty transport builder.
     */
    public Builder withTcpKeepAlive() {
      return withTcpKeepAlive(true);
    }

    /**
     * Sets the SO_KEEPALIVE option.
     *
     * @param tcpKeepAlive Whether to enable SO_KEEPALIVE.
     * @return The Netty transport builder.
     */
    public Builder withTcpKeepAlive(boolean tcpKeepAlive) {
      builder.withTcpKeepAlive(tcpKeepAlive);
      return this;
    }

    /**
     * Enables the TCP_NODELAY option.
     *
     * @return The Netty transport builder.
     */
    public Builder withTcpNoDelay() {
      return withTcpNoDelay(true);
    }

    /**
     * Sets the TCP_NODELAY option.
     *
     * @param tcpNoDelay Whether to enable TCP_NODELAY.
     * @return The Netty transport builder.
     */
    public Builder withTcpNoDelay(boolean tcpNoDelay) {
      builder.withTcpNoDelay(tcpNoDelay);
      return this;
    }

    /**
     * Sets the TCP accept backlog.
     *
     * @param acceptBacklog The accept backlog.
     * @return The Netty transport builder.
     */
    public Builder withAcceptBacklog(int acceptBacklog) {
      builder.withAcceptBacklog(acceptBacklog);
      return this;
    }

    /**
     * Sets the request timeout.
     *
     * @param requestTimeout The request timeout.
     * @return The Netty transport builder.
     */
    public Builder withRequestTimeout(int requestTimeout) {
      builder.withRequestTimeout(requestTimeout);
      return this;
    }

    /**
     * Sets the SSL options.
     *
     * @param sslOptions The SSL options.
     * @return The Netty protocol builder.
     */
    public Builder withSsl(SslOptions sslOptions) {
      builder.withSsl(sslOptions);
      return this;
    }

    @Override
    public NettyTcpProtocol build() {
      return new NettyTcpProtocol(builder.build());
    }
  }
}
