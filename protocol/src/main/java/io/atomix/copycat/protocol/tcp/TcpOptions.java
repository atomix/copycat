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

import io.atomix.copycat.protocol.ssl.SslOptions;
import io.atomix.copycat.util.Assert;

/**
 * Netty TCP options.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class TcpOptions {

  /**
   * Returns a new Netty TCP options builder.
   *
   * @return A new Netty TCP options builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  private static final int DEFAULT_THREADS = Runtime.getRuntime().availableProcessors();
  private static final int DEFAULT_CONNECT_TIMEOUT = 5000;
  private static final int DEFAULT_SEND_BUFFER_SIZE = -1;
  private static final int DEFAULT_RECEIVE_BUFFER_SIZE = -1;
  private static final int DEFAULT_MAX_FRAME_SIZE = 64 * 1024 * 1024;
  private static final boolean DEFAULT_REUSE_ADDRESS = true;
  private static final boolean DEFAULT_TCP_KEEP_ALIVE = true;
  private static final boolean DEFAULT_TCP_NO_DELAY = false;
  private static final int DEFAULT_ACCEPT_BACKLOG = 1024;
  private static final int DEFAULT_REQUEST_TIMEOUT = 500;

  private int threads = DEFAULT_THREADS;
  private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
  private int sendBufferSize = DEFAULT_SEND_BUFFER_SIZE;
  private int receiveBufferSize = DEFAULT_RECEIVE_BUFFER_SIZE;
  private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
  private boolean reuseAddress = DEFAULT_REUSE_ADDRESS;
  private boolean tcpKeepAlive = DEFAULT_TCP_KEEP_ALIVE;
  private boolean tcpNoDelay = DEFAULT_TCP_NO_DELAY;
  private int acceptBacklog = DEFAULT_ACCEPT_BACKLOG;
  private int requestTimeout = DEFAULT_REQUEST_TIMEOUT;
  private SslOptions sslOptions = new SslOptions();

  private TcpOptions() {
  }

  /**
   * The number of event loop threads.
   */
  public int threads() {
    return threads;
  }

  /**
   * The connect timeout in milliseconds.
   */
  public int connectTimeout() {
    return connectTimeout;
  }

  /**
   * The TCP send buffer size.
   */
  public int sendBufferSize() {
    return sendBufferSize;
  }

  /**
   * The TCP receive buffer size.
   */
  public int receiveBufferSize() {
    return receiveBufferSize;
  }

  /**
   * The maximum frame size.
   */
  public int maxFrameSize() {
    return maxFrameSize;
  }

  /**
   * The SO_REUSEADDR option.
   */
  public boolean reuseAddress() {
    return reuseAddress;
  }

  /**
   * The SO_KEEPALIVE option.
   */
  public boolean tcpKeepAlive() {
    return tcpKeepAlive;
  }

  /**
   * The TCP_NODELAY option.
   */
  public boolean tcpNoDelay() {
    return tcpNoDelay;
  }

  /**
   * The TCP accept backlog.
   */
  public int acceptBacklog() {
    return acceptBacklog;
  }

  /**
   * The request timeout.
   */
  public int requestTimeout() {
    return requestTimeout;
  }

  /**
   * The SSL options.
   */
  public SslOptions ssl() {
    return sslOptions;
  }

  /**
   * TCP options builder.
   */
  public static class Builder implements io.atomix.copycat.util.Builder<TcpOptions> {
    private final TcpOptions options = new TcpOptions();

    private Builder() {
    }

    /**
     * Sets the number of Netty event loop threads.
     *
     * @param threads The number of Netty event loop threads.
     * @return The Netty transport builder.
     */
    public Builder withThreads(int threads) {
      options.threads = Assert.argNot(threads, threads <= 0, "threads must be positive");
      return this;
    }

    /**
     * Sets the Netty connect timeout.
     *
     * @param connectTimeout The connect timeout.
     * @return The Netty transport builder.
     */
    public Builder withConnectTimeout(int connectTimeout) {
      options.connectTimeout = Assert.argNot(connectTimeout, connectTimeout <= 0, "connect timeout must be positive");
      return this;
    }

    /**
     * Sets the send buffer size.
     *
     * @param sendBufferSize The send buffer size.
     * @return The Netty transport builder.
     */
    public Builder withSendBufferSize(int sendBufferSize) {
      options.sendBufferSize = Assert.argNot(sendBufferSize, sendBufferSize <= 0, "buffer size must be positive");
      return this;
    }

    /**
     * Sets the receive buffer size.
     *
     * @param receiveBufferSize The receive buffer size.
     * @return The Netty transport builder.
     */
    public Builder withReceiveBufferSize(int receiveBufferSize) {
      options.receiveBufferSize = Assert.argNot(receiveBufferSize, receiveBufferSize <= 0, "buffer size must be positive");
      return this;
    }

    /**
     * Sets the maximum frame size.
     *
     * @param maxFrameSize The maximum frame size.
     * @return The Netty transport builder.
     */
    public Builder withMaxFrameSize(int maxFrameSize) {
      options.maxFrameSize = Assert.argNot(maxFrameSize, maxFrameSize <= 0, "maximum frame size must be positive");
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
      options.reuseAddress = reuseAddress;
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
      options.tcpKeepAlive = tcpKeepAlive;
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
      options.tcpNoDelay = tcpNoDelay;
      return this;
    }

    /**
     * Sets the TCP accept backlog.
     *
     * @param acceptBacklog The accept backlog.
     * @return The Netty transport builder.
     */
    public Builder withAcceptBacklog(int acceptBacklog) {
      options.acceptBacklog = Assert.argNot(acceptBacklog, acceptBacklog <= 0, "accept backlog must be positive");
      return this;
    }

    /**
     * Sets the request timeout.
     *
     * @param requestTimeout The request timeout.
     * @return The Netty transport builder.
     */
    public Builder withRequestTimeout(int requestTimeout) {
      options.requestTimeout = Assert.argNot(requestTimeout, requestTimeout <= 0, "request timeout must be positive");
      return this;
    }

    /**
     * Sets the SSL options.
     *
     * @param sslOptions The SSL options.
     * @return The Netty transport builder.
     */
    public Builder withSsl(SslOptions sslOptions) {
      options.sslOptions = sslOptions;
      return this;
    }

    @Override
    public TcpOptions build() {
      return options;
    }
  }
}
