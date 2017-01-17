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
package io.atomix.copycat.protocol.tcp.request;

import io.atomix.copycat.protocol.request.ProtocolRequest;

/**
 * Base interface for requests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface NetSocketRequest extends ProtocolRequest {

  /**
   * Protocol request type.
   */
  enum Type {
    CONNECT_REQUEST(0x00, NetSocketConnectRequest.class),
    REGISTER_REQUEST(0x01, NetSocketRegisterRequest.class),
    KEEP_ALIVE_REQUEST(0x02, NetSocketKeepAliveRequest.class),
    UNREGISTER_REQUEST(0x03, NetSocketUnregisterRequest.class),
    QUERY_REQUEST(0x04, NetSocketQueryRequest.class),
    COMMAND_REQUEST(0x05, NetSocketCommandRequest.class),
    PUBLISH_REQUEST(0x06, NetSocketPublishRequest.class);

    private final int id;
    private final Class<? extends NetSocketRequest> type;

    Type(int id, Class<? extends NetSocketRequest> type) {
      this.id = id;
      this.type = type;
    }

    /**
     * Returns the request type ID.
     *
     * @return The request type ID.
     */
    public int id() {
      return id;
    }

    /**
     * Returns the request type class.
     *
     * @return The request type class.
     */
    public Class<? extends NetSocketRequest> type() {
      return type;
    }

    /**
     * Returns a boolean indicating whether the given type is a protocol request type.
     *
     * @param id The id to check.
     * @return Indicates whether the given type is a protocol request type.
     */
    public static boolean isProtocolRequest(int id) {
      switch (id) {
        case 0x00:
        case 0x01:
        case 0x02:
        case 0x03:
        case 0x04:
        case 0x05:
        case 0x06:
          return true;
        default:
          return false;
      }
    }

    /**
     * Returns the request type for the given ID.
     *
     * @param id The request type ID.
     * @return The request type.
     */
    public static Type forId(int id) {
      switch (id) {
        case 0x00:
          return CONNECT_REQUEST;
        case 0x01:
          return REGISTER_REQUEST;
        case 0x02:
          return KEEP_ALIVE_REQUEST;
        case 0x03:
          return UNREGISTER_REQUEST;
        case 0x04:
          return QUERY_REQUEST;
        case 0x05:
          return COMMAND_REQUEST;
        case 0x06:
          return PUBLISH_REQUEST;
        default:
          throw new IllegalArgumentException("Unknown request type: " + id);
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
  interface Builder<T extends Builder<T, U>, U extends NetSocketRequest> extends ProtocolRequest.Builder<T, U> {
  }

}
