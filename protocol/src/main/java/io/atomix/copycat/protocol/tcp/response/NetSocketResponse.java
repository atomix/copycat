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
package io.atomix.copycat.protocol.tcp.response;

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
public interface NetSocketResponse extends ProtocolResponse {

  /**
   * Protocol response type.
   */
  enum Type {
    CONNECT_RESPONSE(0x0A, NetSocketConnectResponse.class),
    REGISTER_RESPONSE(0x0B, NetSocketRegisterResponse.class),
    KEEP_ALIVE_RESPONSE(0x0C, NetSocketKeepAliveResponse.class),
    UNREGISTER_RESPONSE(0x0D, NetSocketUnregisterResponse.class),
    QUERY_RESPONSE(0x0E, NetSocketQueryResponse.class),
    COMMAND_RESPONSE(0x0F, NetSocketCommandResponse.class),
    PUBLISH_RESPONSE(0x10, NetSocketPublishResponse.class);

    private final int id;
    private final Class<? extends NetSocketResponse> type;

    Type(int id, Class<? extends NetSocketResponse> type) {
      this.id = id;
      this.type = type;
    }

    /**
     * Returns the response type ID.
     *
     * @return The response type ID.
     */
    public int id() {
      return id;
    }

    /**
     * Returns the response type class.
     *
     * @return The response type class.
     */
    public Class<? extends NetSocketResponse> type() {
      return type;
    }

    /**
     * Returns a boolean indicating whether the given type is a protocol response type.
     *
     * @param id The id to check.
     * @return Indicates whether the given type is a protocol response type.
     */
    public static boolean isProtocolResponse(int id) {
      switch (id) {
        case 0x0a:
        case 0x0b:
        case 0x0c:
        case 0x0d:
        case 0x0e:
        case 0x0f:
        case 0x10:
          return true;
        default:
          return false;
      }
    }

    /**
     * Returns the response type for the given ID.
     *
     * @param id The response type ID.
     * @return The response type.
     */
    public static Type forId(int id) {
      switch (id) {
        case 0x0a:
          return CONNECT_RESPONSE;
        case 0x0b:
          return REGISTER_RESPONSE;
        case 0x0c:
          return KEEP_ALIVE_RESPONSE;
        case 0x0d:
          return UNREGISTER_RESPONSE;
        case 0x0e:
          return QUERY_RESPONSE;
        case 0x0f:
          return COMMAND_RESPONSE;
        case 0x10:
          return PUBLISH_RESPONSE;
        default:
          throw new IllegalArgumentException("Unknown response type: " + id);
      }
    }
  }

  /**
   * Returns the response ID.
   *
   * @return The response ID.
   */
  long id();

  /**
   * Returns the protocol response type.
   *
   * @return The protocol response type.
   */
  Type type();

  /**
   * Response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  interface Builder<T extends Builder<T, U>, U extends NetSocketResponse> extends ProtocolResponse.Builder<T, U> {
    /**
     * Sets the response ID.
     *
     * @param id The response ID.
     * @return The response builder.
     */
    T withId(long id);
  }

}
