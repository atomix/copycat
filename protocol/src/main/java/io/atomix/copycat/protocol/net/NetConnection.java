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
package io.atomix.copycat.protocol.net;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.protocol.ProtocolConnection;
import io.atomix.copycat.protocol.net.request.NetRequest;
import io.atomix.copycat.protocol.net.response.NetResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.util.concurrent.Listener;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import io.vertx.core.parsetools.RecordParser;
import org.slf4j.Logger;

import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * TCP protocol connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class NetConnection implements ProtocolConnection {
  private final NetSocket socket;
  private final RecordParser parser = RecordParser.newFixed(4, null);
  private int size = -1;
  private Consumer<Throwable> exceptionListener;
  private Consumer<ProtocolConnection> closeListener;
  protected final Kryo kryo = new Kryo();
  private final Map<Long, CompletableFuture> futures = new ConcurrentHashMap<>();

  protected NetConnection(NetSocket socket) {
    this.socket = socket;
    parser.setOutput(this::handleBuffer);
    socket.handler(parser);
    socket.exceptionHandler(this::handleException);
    socket.closeHandler(this::handleClose);
  }

  /**
   * Returns the connection logger.
   *
   * @return The connection logger.
   */
  protected abstract Logger logger();

  /**
   * Handles a buffered message.
   */
  private void handleBuffer(Buffer buffer) {
    if (size == -1) {
      size = buffer.getInt(0);
      parser.fixedSizeMode(size);
    } else {
      handleMessage(buffer.getByte(0), buffer.getBytes(1, size));
      size = -1;
    }
  }

  /**
   * Handles a message.
   */
  protected void handleMessage(int id, byte[] bytes) {
    if (NetRequest.Types.isProtocolRequest(id)) {
      NetRequest.Type type = NetRequest.Types.forId(id);
      NetRequest request = kryo.readObject(new Input(bytes), type.type(), type.serializer().get());
      onRequest(request);
    } else if (NetResponse.Types.isProtocolResponse(id)) {
      NetResponse.Type type = NetResponse.Types.forId(id);
      NetResponse response = kryo.readObject(new Input(bytes), type.type(), type.serializer().get());
      onResponse(response);
    }
  }

  /**
   * Called when a request is received.
   */
  protected abstract boolean onRequest(NetRequest request);

  /**
   * Called when a response is received.
   */
  @SuppressWarnings("unchecked")
  protected void onResponse(NetResponse response) {
    CompletableFuture future = futures.remove(response.id());
    if (future != null) {
      future.complete(response);
    }
  }

  /**
   * Sends a web socket request.
   */
  protected <T extends NetRequest, U extends ProtocolResponse> CompletableFuture<U> sendRequest(T request) {
    CompletableFuture<U> future = new CompletableFuture<>();
    futures.put(request.id(), future);
    logger().debug("Sending {}", request);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Output output = new Output(os);
    kryo.writeObject(output, request, request.type().serializer().get());
    byte[] bytes = os.toByteArray();
    Buffer buffer = Buffer.buffer()
      .appendInt(1 + bytes.length)
      .appendByte((byte) request.type().id())
      .appendBytes(bytes);
    socket.write(buffer);
    return future;
  }

  /**
   * Sends a web socket response.
   */
  protected <T extends NetResponse> void sendResponse(T response) {
    logger().debug("Sending {}", response);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    Output output = new Output(os);
    kryo.writeObject(output, response, response.type().serializer().get());
    byte[] bytes = os.toByteArray();
    Buffer buffer = Buffer.buffer()
      .appendInt(1 + bytes.length)
      .appendByte((byte) response.type().id())
      .appendBytes(bytes);
    socket.write(buffer);
  }

  /**
   * Handles a socket exception.
   */
  private void handleException(Throwable error) {
    Consumer<Throwable> listener = this.exceptionListener;
    if (listener != null) {
      listener.accept(error);
    }
  }

  /**
   * Handles a socket closed event.
   */
  private void handleClose(Void v) {
    Consumer<ProtocolConnection> listener = this.closeListener;
    if (listener != null) {
      listener.accept(this);
    }
  }

  @Override
  public Listener<Throwable> exceptionListener(Consumer<Throwable> listener) {
    this.exceptionListener = listener;
    return new Listener<Throwable>() {
      @Override
      public void accept(Throwable throwable) {
        listener.accept(throwable);
      }
      @Override
      public void close() {
        exceptionListener = null;
      }
    };
  }

  @Override
  public Listener<ProtocolConnection> closeListener(Consumer<ProtocolConnection> listener) {
    this.closeListener = listener;
    return new Listener<ProtocolConnection>() {
      @Override
      public void accept(ProtocolConnection connection) {
        listener.accept(connection);
      }
      @Override
      public void close() {
        closeListener = null;
      }
    };
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    Consumer<ProtocolConnection> closeListener = this.closeListener;
    socket.closeHandler(v -> {
      if (closeListener != null) {
        closeListener.accept(this);
      }
      future.complete(null);
    });
    socket.close();
    return future;
  }
}
