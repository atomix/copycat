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
public interface WebSocketResponse extends ProtocolResponse {

  /**
   * Protocol request type.
   */
  interface Type {
    /**
     * Returns the request type class.
     */
    Class<? extends WebSocketResponse> type();
  }

  /**
   * Protocol response type.
   */
  enum Types implements Type {
    CONNECT_RESPONSE(WebSocketConnectResponse.class),
    REGISTER_RESPONSE(WebSocketRegisterResponse.class),
    KEEP_ALIVE_RESPONSE(WebSocketKeepAliveResponse.class),
    UNREGISTER_RESPONSE(WebSocketUnregisterResponse.class),
    QUERY_RESPONSE(WebSocketQueryResponse.class),
    COMMAND_RESPONSE(WebSocketCommandResponse.class),
    PUBLISH_RESPONSE(WebSocketPublishResponse.class);

    private final Class<? extends WebSocketResponse> type;

    Types(Class<? extends WebSocketResponse> type) {
      this.type = type;
    }

    @Override
    public Class<? extends WebSocketResponse> type() {
      return type;
    }

    /**
     * Returns a boolean indicating whether the given type is a protocol response type.
     *
     * @param type The type to check.
     * @return Indicates whether the given type is a protocol response type.
     */
    public static boolean isProtocolResponse(String type) {
      switch (type) {
        case "CONNECT_RESPONSE":
        case "REGISTER_RESPONSE":
        case "KEEP_ALIVE_RESPONSE":
        case "UNREGISTER_RESPONSE":
        case "QUERY_RESPONSE":
        case "COMMAND_RESPONSE":
        case "PUBLISH_RESPONSE":
          return true;
        default:
          return false;
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
