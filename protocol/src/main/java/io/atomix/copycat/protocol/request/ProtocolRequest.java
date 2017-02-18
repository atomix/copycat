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
package io.atomix.copycat.protocol.request;

/**
 * Local request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ProtocolRequest {

  /**
   * Protocol request type.
   */
  class Type {
    public static final Type CONNECT    = new Type(0x00, ConnectRequest.class);
    public static final Type REGISTER   = new Type(0x01, RegisterRequest.class);
    public static final Type KEEP_ALIVE = new Type(0x02, KeepAliveRequest.class);
    public static final Type UNREGISTER = new Type(0x03, UnregisterRequest.class);
    public static final Type QUERY      = new Type(0x04, QueryRequest.class);
    public static final Type COMMAND    = new Type(0x05, CommandRequest.class);
    public static final Type PUBLISH    = new Type(0x06, PublishRequest.class);

    private final int id;
    private final Class<?> type;

    protected Type(int id, Class<?> type) {
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
     * Returns the request class.
     *
     * @return The request class.
     */
    public Class type() {
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
  interface Builder<T extends Builder<T, U>, U extends ProtocolRequest> extends io.atomix.copycat.util.Builder<U> {
    /**
     * Creates a copy of the given request.
     *
     * @param request The request to copy.
     * @return The copied request.
     */
    U copy(U request);
  }
}
