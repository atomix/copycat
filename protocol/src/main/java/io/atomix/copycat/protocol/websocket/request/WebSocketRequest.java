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

import io.atomix.copycat.protocol.request.ProtocolRequest;

/**
 * Base interface for requests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface WebSocketRequest extends ProtocolRequest {

  /**
   * Protocol request type.
   */
  enum Type {
    CONNECT_REQUEST(WebSocketConnectRequest.class),
    REGISTER_REQUEST(WebSocketRegisterRequest.class),
    KEEP_ALIVE_REQUEST(WebSocketKeepAliveRequest.class),
    UNREGISTER_REQUEST(WebSocketUnregisterRequest.class),
    QUERY_REQUEST(WebSocketQueryRequest.class),
    COMMAND_REQUEST(WebSocketCommandRequest.class),
    PUBLISH_REQUEST(WebSocketPublishRequest.class);

    private final Class<? extends WebSocketRequest> type;

    Type(Class<? extends WebSocketRequest> type) {
      this.type = type;
    }

    /**
     * Returns the request type class.
     *
     * @return The request type class.
     */
    public Class<? extends WebSocketRequest> type() {
      return type;
    }

    /**
     * Returns a boolean indicating whether the given type is a protocol request type.
     *
     * @param type The type to check.
     * @return Indicates whether the given type is a protocol request type.
     */
    public static boolean isProtocolRequest(String type) {
      switch (type) {
        case "CONNECT_REQUEST":
        case "REGISTER_REQUEST":
        case "KEEP_ALIVE_REQUEST":
        case "UNREGISTER_REQUEST":
        case "QUERY_REQUEST":
        case "COMMAND_REQUEST":
        case "PUBLISH_REQUEST":
          return true;
        default:
          return false;
      }
    }
  }

  /**
   * Returns the request ID.
   *
   * @return The request ID.
   */
  long id();

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
