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

import io.atomix.copycat.protocol.ProtocolConnection;
import io.atomix.copycat.protocol.ProtocolListener;
import io.atomix.copycat.protocol.request.ProtocolRequest;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.serializers.ProtocolRequestSerializer;
import io.atomix.copycat.protocol.serializers.ProtocolResponseSerializer;
import io.atomix.copycat.util.concurrent.Listener;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;

import java.net.ConnectException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Netty TCP connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class NettyTcpConnection implements ProtocolConnection {
  protected static final ThreadLocal<ByteBufInput> INPUT = new ThreadLocal<ByteBufInput>() {
    @Override
    protected ByteBufInput initialValue() {
      return new ByteBufInput();
    }
  };
  protected static final ThreadLocal<ByteBufOutput> OUTPUT = new ThreadLocal<ByteBufOutput>() {
    @Override
    protected ByteBufOutput initialValue() {
      return new ByteBufOutput();
    }
  };

  protected final Channel channel;
  private final long requestTimeout;
  private Consumer<Throwable> exceptionListener;
  private Consumer<ProtocolConnection> closeListener;
  private final AtomicLong requestId = new AtomicLong();
  private final Map<Long, ResponseFuture> responseFutures = new ConcurrentHashMap<>();
  private final AtomicBoolean closed = new AtomicBoolean();
  private final ScheduledFuture<?> timeout;
  private ChannelFuture writeFuture;

  public NettyTcpConnection(Channel channel, TcpOptions options) {
    this.channel = channel;
    this.requestTimeout = options.requestTimeout();
    this.timeout = channel.eventLoop().scheduleWithFixedDelay(this::timeout, requestTimeout, requestTimeout, TimeUnit.MILLISECONDS);
  }

  /**
   * Returns the logger for the connection.
   *
   * @return The logger for the connection.
   */
  protected abstract Logger logger();

  /**
   * Called when a message is received.
   */
  protected void onMessage(ByteBuf buffer) {
    final long id = buffer.readLong();
    final byte typeId = buffer.readByte();
    if (ProtocolRequest.Type.isProtocolRequest(typeId)) {
      onRequest(id, readRequest(typeId, buffer));
    } else if (ProtocolResponse.Type.isProtocolResponse(typeId)) {
      onResponse(id, readResponse(typeId, buffer));
    }
  }

  /**
   * Called when a request is received.
   */
  protected abstract boolean onRequest(long id, ProtocolRequest request);

  /**
   * Handles a protocol request.
   */
  protected <T extends ProtocolRequest, U extends ProtocolResponse> void onRequest(long id, T request, ProtocolListener<T, U> listener) {
    if (listener != null) {
      listener.onRequest(request).whenComplete((response, error) -> {
        if (error == null) {
          sendResponse(id, response);
        }
      });
    }
  }

  /**
   * Serializes a request.
   */
  @SuppressWarnings("unchecked")
  protected void writeRequest(ProtocolRequest request, ByteBuf buffer) {
    ProtocolRequestSerializer serializer = ProtocolRequestSerializer.forType(request.type());
    serializer.writeObject(OUTPUT.get().setByteBuf(buffer), request);
  }

  /**
   * Deserializes a request.
   */
  @SuppressWarnings("unchecked")
  protected ProtocolRequest readRequest(int typeId, ByteBuf buffer) {
    ProtocolRequest.Type type = ProtocolRequest.Type.forId(typeId);
    ProtocolRequestSerializer<?> serializer = ProtocolRequestSerializer.forType(type);
    return serializer.readObject(INPUT.get().setByteBuf(buffer), type.type());
  }

  /**
   * Serializes a response.
   */
  @SuppressWarnings("unchecked")
  protected void writeResponse(ProtocolResponse response, ByteBuf buffer) {
    ProtocolResponseSerializer serializer = ProtocolResponseSerializer.forType(response.type());
    serializer.writeObject(OUTPUT.get().setByteBuf(buffer), response);
  }

  /**
   * Deserializes a response.
   */
  @SuppressWarnings("unchecked")
  protected ProtocolResponse readResponse(int typeId, ByteBuf buffer) {
    ProtocolResponse.Type type = ProtocolResponse.Type.forId(typeId);
    ProtocolResponseSerializer<?> serializer = ProtocolResponseSerializer.forType(type);
    return serializer.readObject(INPUT.get().setByteBuf(buffer), type.type());
  }

  /**
   * Sends a web socket request.
   */
  @SuppressWarnings("unchecked")
  protected <T extends ProtocolRequest, U extends ProtocolResponse> CompletableFuture<U> sendRequest(T request) {
    final long id = requestId.incrementAndGet();
    ResponseFuture<U> future = new ResponseFuture<>(System.currentTimeMillis());
    responseFutures.put(id, future);
    logger().debug("Sending {}", request);
    ByteBuf buffer = channel.alloc().buffer(9)
      .writeLong(id)
      .writeByte(request.type().id());
    writeRequest(request, buffer);
    channel.writeAndFlush(buffer);
    return future;
  }

  /**
   * Sends a web socket response.
   */
  @SuppressWarnings("unchecked")
  protected <T extends ProtocolResponse> void sendResponse(long id, T response) {
    logger().debug("Sending {}", response);
    ByteBuf buffer = channel.alloc().buffer(9)
      .writeLong(id)
      .writeByte(response.type().id());
    writeResponse(response, buffer);
    channel.writeAndFlush(buffer);
  }

  /**
   * Called when a response is received.
   */
  @SuppressWarnings("unchecked")
  protected void onResponse(long id, ProtocolResponse response) {
    CompletableFuture future = responseFutures.remove(id);
    if (future != null) {
      future.complete(response);
    }
  }

  /**
   * Times out requests.
   */
  void timeout() {
    long time = System.currentTimeMillis();
    Iterator<Map.Entry<Long, ResponseFuture>> iterator = responseFutures.entrySet().iterator();
    while (iterator.hasNext()) {
      ResponseFuture future = iterator.next().getValue();
      if (future.time + requestTimeout < time) {
        iterator.remove();
        future.completeExceptionally(new TimeoutException("request timed out"));
      } else {
        break;
      }
    }
  }

  @Override
  public Listener<Throwable> onException(Consumer<Throwable> listener) {
    this.exceptionListener = listener;
    return new Listener<Throwable>() {
      @Override
      public void accept(Throwable t) {
        listener.accept(t);
      }

      @Override
      public void close() {
        exceptionListener = null;
      }
    };
  }

  protected void onException(Throwable t) {
    Consumer<Throwable> exceptionListener = this.exceptionListener;
    if (exceptionListener != null) {
      exceptionListener.accept(t);
    }
  }

  @Override
  public Listener<ProtocolConnection> onClose(Consumer<ProtocolConnection> listener) {
    this.closeListener = listener;
    return new Listener<ProtocolConnection>() {
      @Override
      public void accept(ProtocolConnection connection) {
        listener.accept(connection);
      }

      @Override
      public void close() {
        exceptionListener = null;
      }
    };
  }

  protected void onClose() {
    if (closed.compareAndSet(false, true)) {
      for (CompletableFuture<?> responseFuture : responseFutures.values()) {
        responseFuture.completeExceptionally(new ConnectException("connection closed"));
      }
      responseFutures.clear();


      Consumer<ProtocolConnection> closeListener = this.closeListener;
      if (closeListener != null) {
        closeListener.accept(this);
      }
      timeout.cancel(true);
    }
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    if (writeFuture != null && !writeFuture.isDone()) {
      writeFuture.addListener(channelFuture -> {
        channel.close().addListener(closeFuture -> {
          if (closeFuture.isSuccess()) {
            future.complete(null);
          } else {
            future.completeExceptionally(closeFuture.cause());
          }
        });
      });
    } else {
      channel.close().addListener(closeFuture -> {
        if (closeFuture.isSuccess()) {
          future.complete(null);
        } else {
          future.completeExceptionally(closeFuture.cause());
        }
      });
    }
    return future;
  }

  @Override
  public int hashCode() {
    return channel.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof NettyTcpConnection && ((NettyTcpConnection) object).channel.equals(channel);
  }

  /**
   * Response future.
   */
  private static class ResponseFuture<T> extends CompletableFuture<T> {
    private final long time;

    private ResponseFuture(long time) {
      this.time = time;
    }
  }

}
