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
package io.atomix.copycat.protocol.net.response;

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
public interface NetResponse<T extends NetResponse<T>> extends ProtocolResponse {

  /**
   * TCP response type.
   */
  class Type<T extends NetResponse<T>> {
    public static final Type<NetConnectResponse>       CONNECT = new Type<>(0x10, NetConnectResponse.class, new NetConnectResponse.Serializer());
    public static final Type<NetRegisterResponse>     REGISTER = new Type<>(0x11, NetRegisterResponse.class, new NetRegisterResponse.Serializer());
    public static final Type<NetKeepAliveResponse>  KEEP_ALIVE = new Type<>(0x12, NetKeepAliveResponse.class, new NetKeepAliveResponse.Serializer());
    public static final Type<NetUnregisterResponse> UNREGISTER = new Type<>(0x13, NetUnregisterResponse.class, new NetUnregisterResponse.Serializer());
    public static final Type<NetQueryResponse>           QUERY = new Type<>(0x14, NetQueryResponse.class, new NetQueryResponse.Serializer());
    public static final Type<NetCommandResponse>       COMMAND = new Type<>(0x15, NetCommandResponse.class, new NetCommandResponse.Serializer());
    public static final Type<NetPublishResponse>       PUBLISH = new Type<>(0x16, NetPublishResponse.class, new NetPublishResponse.Serializer());

    private final int id;
    private final Class<T> type;
    private final Serializer<T> serializer;

    protected Type(int id, Class<T> type, Serializer<T> serializer) {
      this.id = id;
      this.type = type;
      this.serializer = serializer;
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
     * Returns the response class.
     *
     * @return The response class.
     */
    public Class<T> type() {
      return type;
    }

    /**
     * Returns the response type serializer.
     *
     * @return The response type serializer.
     */
    public Serializer<T> serializer() {
      return serializer;
    }

    /**
     * Returns a boolean indicating whether the given type is a protocol response type.
     *
     * @param id The id to check.
     * @return Indicates whether the given type is a protocol response type.
     */
    public static boolean isProtocolResponse(int id) {
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
     * Returns the response type for the given ID.
     *
     * @param id The response type ID.
     * @return The response type.
     */
    public static Type<?> forId(int id) {
      switch (id) {
        case 0x00:
          return CONNECT;
        case 0x01:
          return REGISTER;
        case 0x02:
          return KEEP_ALIVE;
        case 0x03:
          return UNREGISTER;
        case 0x04:
          return QUERY;
        case 0x05:
          return COMMAND;
        case 0x06:
          return PUBLISH;
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
  interface Builder<T extends Builder<T, U>, U extends NetResponse> extends ProtocolResponse.Builder<T, U> {
    /**
     * Sets the response ID.
     *
     * @param id The response ID.
     * @return The response builder.
     */
    T withId(long id);
  }

  /**
   * Response serializer.
   */
  abstract class Serializer<T extends NetResponse> extends com.esotericsoftware.kryo.Serializer<T> {
  }

}
