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
package io.atomix.copycat.protocol.websocket.response;

import com.fasterxml.jackson.annotation.JsonGetter;
import io.atomix.copycat.protocol.response.ProtocolResponse;

/**
 * Base interface for responses.
 * <p>
 * Each response has a non-null {@link Status} of either {@link Status#OK} or
 * {@link Status#ERROR}. Responses where {@link #status()} is {@link Status#ERROR}
 * may provide an optional {@link #error()} code.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface WebSocketResponse<T extends WebSocketResponse<T>> extends ProtocolResponse {
  String TYPE = "response";

  /**
   * Web socket response type.
   */
  class Type<T extends WebSocketResponse<T>> {
    public static final Type<WebSocketConnectResponse>       CONNECT = new Type<>("connect", WebSocketConnectResponse.class);
    public static final Type<WebSocketRegisterResponse>     REGISTER = new Type<>("register", WebSocketRegisterResponse.class);
    public static final Type<WebSocketKeepAliveResponse>  KEEP_ALIVE = new Type<>("keep_alive", WebSocketKeepAliveResponse.class);
    public static final Type<WebSocketUnregisterResponse> UNREGISTER = new Type<>("unregister", WebSocketUnregisterResponse.class);
    public static final Type<WebSocketQueryResponse>           QUERY = new Type<>("query", WebSocketQueryResponse.class);
    public static final Type<WebSocketCommandResponse>       COMMAND = new Type<>("command", WebSocketCommandResponse.class);
    public static final Type<WebSocketPublishResponse>       PUBLISH = new Type<>("publish", WebSocketPublishResponse.class);

    private final String name;
    private final Class<T> type;

    public Type(String name, Class<T> type) {
      this.name = name;
      this.type = type;
    }

    /**
     * Returns the response type name.
     *
     * @return The response type name.
     */
    public String name() {
      return name;
    }

    /**
     * Returns the response type class.
     *
     * @return The response type class.
     */
    public Class<T> type() {
      return type;
    }

    /**
     * Returns the web socket response type for the given name.
     *
     * @param name The web socket response type name.
     * @return The web socket response type for the given name.
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
          throw new IllegalArgumentException("unknown response type name: " + name);
      }
    }
  }

  /**
   * Returns the response ID.
   *
   * @return The response ID.
   */
  @JsonGetter("id")
  long id();

  /**
   * Returns the response method.
   *
   * @return The response method.
   */
  @JsonGetter("method")
  default String method() {
    return TYPE;
  }

  /**
   * Returns the protocol response type.
   *
   * @return The protocol response type.
   */
  @JsonGetter("type")
  Type type();

  /**
   * Response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  interface Builder<T extends Builder<T, U>, U extends WebSocketResponse> extends ProtocolResponse.Builder<T, U> {
    /**
     * Sets the response ID.
     *
     * @param id The response ID.
     * @return The response builder.
     */
    T withId(long id);
  }
}
