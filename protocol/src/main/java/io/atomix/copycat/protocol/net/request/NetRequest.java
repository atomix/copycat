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
package io.atomix.copycat.protocol.net.request;

import io.atomix.copycat.protocol.request.ProtocolRequest;

/**
 * Base interface for requests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface NetRequest<T extends NetRequest<T>> extends ProtocolRequest {

  /**
   * TCP request type.
   */
  class Type<T extends NetRequest<T>> {
    public static final Type<NetConnectRequest>       CONNECT = new Type<>(0x00, NetConnectRequest.class, new NetConnectRequest.Serializer());
    public static final Type<NetRegisterRequest>     REGISTER = new Type<>(0x01, NetRegisterRequest.class, new NetRegisterRequest.Serializer());
    public static final Type<NetKeepAliveRequest>  KEEP_ALIVE = new Type<>(0x02, NetKeepAliveRequest.class, new NetKeepAliveRequest.Serializer());
    public static final Type<NetUnregisterRequest> UNREGISTER = new Type<>(0x03, NetUnregisterRequest.class, new NetUnregisterRequest.Serializer());
    public static final Type<NetQueryRequest>           QUERY = new Type<>(0x04, NetQueryRequest.class, new NetQueryRequest.Serializer());
    public static final Type<NetCommandRequest>       COMMAND = new Type<>(0x05, NetCommandRequest.class, new NetCommandRequest.Serializer());
    public static final Type<NetPublishRequest>       PUBLISH = new Type<>(0x06, NetPublishRequest.class, new NetPublishRequest.Serializer());

    private final int id;
    private final Class<T> type;
    private final Serializer<T> serializer;

    protected Type(int id, Class<T> type, Serializer<T> serializer) {
      this.id = id;
      this.type = type;
      this.serializer = serializer;
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
     * Returns the request class.
     *
     * @return The request class.
     */
    public Class<T> type() {
      return type;
    }

    /**
     * Returns the request type serializer.
     *
     * @return The request type serializer.
     */
    public Serializer<T> serializer() {
      return serializer;
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
  interface Builder<T extends Builder<T, U>, U extends NetRequest> extends ProtocolRequest.Builder<T, U> {
  }

  /**
   * Request serializer.
   */
  abstract class Serializer<T extends NetRequest> extends com.esotericsoftware.kryo.Serializer<T> {
  }

}
