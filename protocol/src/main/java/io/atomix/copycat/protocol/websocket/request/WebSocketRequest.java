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
package io.atomix.copycat.protocol.websocket.request;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.atomix.copycat.protocol.request.ProtocolRequest;

/**
 * Base interface for requests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public interface WebSocketRequest<T extends WebSocketRequest<T>> extends ProtocolRequest {
  String TYPE = "request";

  /**
   * Web socket request type.
   */
  class Type<T extends WebSocketRequest<T>> {
    public static final Type<WebSocketConnectRequest>       CONNECT = new Type<>("connect", WebSocketConnectRequest.class);
    public static final Type<WebSocketRegisterRequest>     REGISTER = new Type<>("register", WebSocketRegisterRequest.class);
    public static final Type<WebSocketKeepAliveRequest>  KEEP_ALIVE = new Type<>("keep_alive", WebSocketKeepAliveRequest.class);
    public static final Type<WebSocketUnregisterRequest> UNREGISTER = new Type<>("unregister", WebSocketUnregisterRequest.class);
    public static final Type<WebSocketQueryRequest>           QUERY = new Type<>("query", WebSocketQueryRequest.class);
    public static final Type<WebSocketCommandRequest>       COMMAND = new Type<>("command", WebSocketCommandRequest.class);
    public static final Type<WebSocketPublishRequest>       PUBLISH = new Type<>("publish", WebSocketPublishRequest.class);

    private final String name;
    private final Class<T> type;

    public Type(String name, Class<T> type) {
      this.name = name;
      this.type = type;
    }

    /**
     * Returns the request type name.
     *
     * @return The request type name.
     */
    public String name() {
      return name;
    }

    /**
     * Returns the request type class.
     *
     * @return The request type class.
     */
    public Class<T> type() {
      return type;
    }

    /**
     * Returns the web socket request type for the given name.
     *
     * @param name The web socket request type name.
     * @return The web socket request type for the given name.
     */
    public static Type<?> forName(String name) {
      switch (name) {
        case "connect":
          return CONNECT;
        case "register":
          return REGISTER;
        case "keep_alive":
          return KEEP_ALIVE;
        case "unregister":
          return UNREGISTER;
        case "query":
          return QUERY;
        case "command":
          return COMMAND;
        case "publish":
          return PUBLISH;
        default:
          throw new IllegalArgumentException("unknown request type name: " + name);
      }
    }
  }

  /**
   * Returns the request ID.
   *
   * @return The request ID.
   */
  @JsonGetter("id")
  long id();

  /**
   * Returns the request method.
   *
   * @return The request method.
   */
  @JsonGetter("method")
  default String method() {
    return TYPE;
  }

  /**
   * Returns the request type.
   *
   * @return The request type.
   */
  Type type();

  /**
   * Request builder.
   *
   * @param <T> The builder type.
   */
  interface Builder<T extends Builder<T, U>, U extends WebSocketRequest> extends ProtocolRequest.Builder<T, U> {
  }
}
