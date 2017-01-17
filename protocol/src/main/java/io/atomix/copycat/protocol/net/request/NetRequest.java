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

import com.esotericsoftware.kryo.Serializer;
import io.atomix.copycat.protocol.request.ProtocolRequest;

import java.util.function.Supplier;

/**
 * Base interface for requests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface NetRequest extends ProtocolRequest {

  /**
   * Protocol request type.
   */
  interface Type {
    /**
     * Returns the request type ID.
     *
     * @return The request type ID.
     */
    int id();

    /**
     * Returns the request type class.
     */
    Class<? extends NetRequest> type();

    /**
     * Returns the request serializer class.
     */
    Supplier<Serializer<?>> serializer();
  }

  /**
   * Protocol request type.
   */
  enum Types implements Type {
    CONNECT_REQUEST(0x00, NetConnectRequest.class, NetConnectRequest.Serializer::new),
    REGISTER_REQUEST(0x01, NetRegisterRequest.class, NetRegisterRequest.Serializer::new),
    KEEP_ALIVE_REQUEST(0x02, NetKeepAliveRequest.class, NetKeepAliveRequest.Serializer::new),
    UNREGISTER_REQUEST(0x03, NetUnregisterRequest.class, NetUnregisterRequest.Serializer::new),
    QUERY_REQUEST(0x04, NetQueryRequest.class, NetQueryRequest.Serializer::new),
    COMMAND_REQUEST(0x05, NetCommandRequest.class, NetCommandRequest.Serializer::new),
    PUBLISH_REQUEST(0x06, NetPublishRequest.class, NetPublishRequest.Serializer::new);

    private final int id;
    private final Class<? extends NetRequest> type;
    private final Supplier<Serializer<?>> serializer;

    Types(int id, Class<? extends NetRequest> type, Supplier<Serializer<?>> serializer) {
      this.id = id;
      this.type = type;
      this.serializer = serializer;
    }

    @Override
    public int id() {
      return id;
    }

    @Override
    public Class<? extends NetRequest> type() {
      return type;
    }

    @Override
    public Supplier<Serializer<?>> serializer() {
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
  interface Builder<T extends Builder<T, U>, U extends NetRequest> extends ProtocolRequest.Builder<T, U> {
  }

}
