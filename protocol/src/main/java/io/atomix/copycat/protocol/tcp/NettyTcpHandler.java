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

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Netty handler.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NettyTcpHandler<T extends NettyTcpConnection> extends ChannelInboundHandlerAdapter {
  private final Function<Channel, T> factory;
  private final Map<Channel, T> connections;
  private final Consumer<T> listener;

  public NettyTcpHandler(Function<Channel, T> factory, Map<Channel, T> connections, Consumer<T> listener) {
    this.factory = factory;
    this.connections = connections;
    this.listener = listener;
  }

  /**
   * Adds a connection for the given channel.
   *
   * @param channel The channel for which to add the connection.
   * @param connection The connection to add.
   */
  protected void setConnection(Channel channel, T connection) {
    connections.put(channel, connection);
  }

  /**
   * Returns the connection for the given channel.
   *
   * @param channel The channel for which to return the connection.
   * @return The connection.
   */
  protected T getConnection(Channel channel) {
    return connections.get(channel);
  }

  /**
   * Removes the connection for the given channel.
   *
   * @param channel The channel for which to remove the connection.
   * @return The connection.
   */
  protected T removeConnection(Channel channel) {
    return connections.remove(channel);
  }

  @Override
  public void channelActive(ChannelHandlerContext context) throws Exception {
    Channel channel = context.channel();
    T connection = factory.apply(channel);
    setConnection(channel, connection);
    listener.accept(connection);
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, Object message) {
    ByteBuf buffer = (ByteBuf) message;
    T connection = getConnection(context.channel());
    if (connection != null) {
      connection.onMessage(buffer);
    }
  }

  @Override
  public void exceptionCaught(ChannelHandlerContext context, final Throwable t) throws Exception {
    Channel channel = context.channel();
    T connection = getConnection(channel);
    if (connection != null) {
      try {
        if (channel.isOpen()) {
          channel.close();
        }
      } catch (Throwable ignore) {
      }
      connection.onException(t);
    } else {
      channel.close();
    }
  }

  @Override
  public void channelInactive(ChannelHandlerContext context) throws Exception {
    Channel channel = context.channel();
    T connection = removeConnection(channel);
    if (connection != null) {
      connection.onClose();
    }
  }

  @Override
  public void userEventTriggered(ChannelHandlerContext context, Object event) throws Exception {
    if (event instanceof IdleStateEvent && ((IdleStateEvent) event).state() == IdleState.ALL_IDLE) {
      context.close();
    }
  }

}
