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
package io.atomix.copycat.protocol.websocket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.copycat.protocol.ProtocolConnection;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.websocket.request.WebSocketRequest;
import io.atomix.copycat.protocol.websocket.response.WebSocketResponse;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocketBase;
import io.vertx.core.http.WebSocketFrame;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Base web socket connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractWebSocketConnection<T extends WebSocketBase> implements ProtocolConnection {
  protected T socket;
  private final ObjectMapper textMapper = new ObjectMapper();
  private final ObjectMapper binaryMapper = new ObjectMapper(new BsonFactory());
  private Consumer<Throwable> exceptionListener;
  private Consumer<ProtocolConnection> closeListener;
  private final Map<Long, CompletableFuture> futures = new ConcurrentHashMap<>();

  public AbstractWebSocketConnection(T socket) {
    this.socket = socket;
    socket.frameHandler(this::handleFrame);
    socket.exceptionHandler(this::handleException);
    socket.closeHandler(this::handleClose);
  }

  protected abstract Logger logger();

  /**
   * Handles a web socket frame.
   */
  private void handleFrame(WebSocketFrame frame) {
    // Only handle binary frames for now.
    if (frame.isFinal()) {
      if (frame.isBinary()) {
        handleBinary(frame.binaryData().getBytes());
      } else if (frame.isText()) {
        handleText(frame.textData());
      }
    }
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

  /**
   * Handles binary data received on the socket.
   */
  private void handleBinary(byte[] data) {
    try {
      handleObject(binaryMapper.readTree(data));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Handles text data received on the socket.
   */
  private void handleText(String data) {
    try {
      handleObject(textMapper.readTree(data));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Handles an object.
   */
  @SuppressWarnings("unchecked")
  private void handleObject(JsonNode json) {
    String type = json.get("type").asText();
    if (WebSocketRequest.Type.isProtocolRequest(type)) {
      WebSocketRequest request = textMapper.convertValue(json, WebSocketRequest.Type.valueOf(type).type());
      logger().debug("Received {}", request);
      onRequest(request);
    } else if (WebSocketResponse.Type.isProtocolResponse(type)) {
      WebSocketResponse response = textMapper.convertValue(json, WebSocketResponse.Type.valueOf(type).type());
      logger().debug("Received {}", response);
      CompletableFuture future = futures.get(response.id());
      if (future != null) {
        future.complete(response);
      }
    }
  }

  /**
   * Sends a web socket request.
   */
  protected <T extends WebSocketRequest, U extends ProtocolResponse> CompletableFuture<U> sendRequest(T request) {
    CompletableFuture<U> future = new CompletableFuture<>();
    futures.put(request.id(), future);
    logger().debug("Sending {}", request);
    socket.writeFinalBinaryFrame(Buffer.buffer(writeBinary(request)));
    return future;
  }

  /**
   * Sends a web socket response.
   */
  protected <T extends WebSocketResponse> void sendResponse(T response) {
    logger().debug("Sending {}", response);
    socket.writeFinalBinaryFrame(Buffer.buffer(writeBinary(response)));
  }

  /**
   * Writes a value as binary.
   */
  private byte[] writeBinary(Object value) {
    try {
      return binaryMapper.writeValueAsBytes(value);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Called when a request is received.
   */
  protected abstract void onRequest(WebSocketRequest request);

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
