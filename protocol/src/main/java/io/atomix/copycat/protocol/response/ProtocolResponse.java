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
package io.atomix.copycat.protocol.response;

import io.atomix.copycat.protocol.error.*;

/**
 * Local response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ProtocolResponse {

  /**
   * Protocol response type.
   */
  class Type {
    public static final Type CONNECT    = new Type(0x10, ConnectResponse.class);
    public static final Type REGISTER   = new Type(0x11, RegisterResponse.class);
    public static final Type KEEP_ALIVE = new Type(0x12, KeepAliveResponse.class);
    public static final Type UNREGISTER = new Type(0x13, UnregisterResponse.class);
    public static final Type QUERY      = new Type(0x14, QueryResponse.class);
    public static final Type COMMAND    = new Type(0x15, CommandResponse.class);
    public static final Type PUBLISH    = new Type(0x16, PublishResponse.class);

    private final int id;
    private final Class<?> type;

    protected Type(int id, Class<?> type) {
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
     * Returns the response class.
     *
     * @return The response class.
     */
    public Class type() {
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
        case 0x10:
        case 0x11:
        case 0x12:
        case 0x13:
        case 0x14:
        case 0x15:
        case 0x16:
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
        case 0x10:
          return CONNECT;
        case 0x11:
          return REGISTER;
        case 0x12:
          return KEEP_ALIVE;
        case 0x13:
          return UNREGISTER;
        case 0x14:
          return QUERY;
        case 0x15:
          return COMMAND;
        case 0x16:
          return PUBLISH;
        default:
          throw new IllegalArgumentException("Unknown response type: " + id);
      }
    }
  }

  /**
   * Response status.
   */
  enum Status {

    /**
     * Indicates a successful response status.
     */
    OK(1),

    /**
     * Indicates a response containing an error.
     */
    ERROR(0);

    /**
     * Returns the status for the given identifier.
     *
     * @param id The status identifier.
     * @return The status for the given identifier.
     * @throws IllegalArgumentException if {@code id} is not 0 or 1
     */
    public static Status forId(int id) {
      switch (id) {
        case 1:
          return OK;
        case 0:
          return ERROR;
        default:
          break;
      }
      throw new IllegalArgumentException("invalid status identifier: " + id);
    }

    private final byte id;

    Status(int id) {
      this.id = (byte) id;
    }

    /**
     * Returns the status identifier.
     *
     * @return The status identifier.
     */
    public byte id() {
      return id;
    }

  }

  /**
   * Response error.
   */
  class Error {
    private final Type type;
    private final String message;

    public Error(Type type, String message) {
      this.type = type;
      this.message = message;
    }

    /**
     * Returns the error type.
     *
     * @return The error type.
     */
    public Type type() {
      return type;
    }

    /**
     * Returns the error message.
     *
     * @return The error message.
     */
    public String message() {
      return message;
    }

    @Override
    public String toString() {
      return String.format("%s[type=%s, message=%s]", getClass().getSimpleName(), type.name(), message);
    }

    /**
     * Error type.
     */
    public enum Type {

      /**
       * No leader error.
       */
      NO_LEADER_ERROR(1) {
        @Override
        public ProtocolException createException(String message) {
          return new NoLeaderException(message != null ? message : "not the leader");
        }
      },

      /**
       * Read application error.
       */
      QUERY_ERROR(2) {
        @Override
        public ProtocolException createException(String message) {
          return new QueryException(message != null ? message : "failed to obtain read quorum");
        }
      },

      /**
       * Write application error.
       */
      COMMAND_ERROR(3) {
        @Override
        public ProtocolException createException(String message) {
          return new CommandException(message != null ? message : "failed to obtain write quorum");
        }
      },

      /**
       * User application error.
       */
      APPLICATION_ERROR(4) {
        @Override
        public ProtocolException createException(String message) {
          return new ApplicationException(message != null ? message : "an application error occurred");
        }
      },

      /**
       * Illegal member state error.
       */
      ILLEGAL_MEMBER_STATE_ERROR(5) {
        @Override
        public ProtocolException createException(String message) {
          return new IllegalMemberStateException(message != null ? message : "illegal member state");
        }
      },

      /**
       * Unknown session error.
       */
      UNKNOWN_SESSION_ERROR(6) {
        @Override
        public ProtocolException createException(String message) {
          return new UnknownSessionException(message != null ? message : "unknown member session");
        }
      },

      /**
       * Internal error.
       */
      INTERNAL_ERROR(7) {
        @Override
        public ProtocolException createException(String message) {
          return new InternalException(message != null ? message : "internal Raft error");
        }
      },

      /**
       * Configuration error.
       */
      CONFIGURATION_ERROR(8) {
        @Override
        public ProtocolException createException(String message) {
          return new ConfigurationException(message != null ? message : "configuration failed");
        }
      };

      private final byte id;

      Type(int id) {
        this.id = (byte) id;
      }

      /**
       * Returns the error type ID.
       *
       * @return The error type ID.
       */
      public byte id() {
        return id;
      }

      /**
       * Creates an exception for the error.
       *
       * @param message The exception message.
       * @return The exception.
       */
      public abstract ProtocolException createException(String message);

      /**
       * Returns the error type for the given ID.
       *
       * @param id The error type ID.
       * @return The error type.
       */
      public static Type forId(int id) {
        switch (id) {
          case 1:
            return NO_LEADER_ERROR;
          case 2:
            return QUERY_ERROR;
          case 3:
            return COMMAND_ERROR;
          case 4:
            return APPLICATION_ERROR;
          case 5:
            return ILLEGAL_MEMBER_STATE_ERROR;
          case 6:
            return UNKNOWN_SESSION_ERROR;
          case 7:
            return INTERNAL_ERROR;
          case 8:
            return CONFIGURATION_ERROR;
          default:
            throw new IllegalArgumentException("unknown error type ID: " + id);
        }
      }
    }

  }

  /**
   * Returns the protocol response type.
   *
   * @return The protocol response type.
   */
  Type type();

  /**
   * Returns the response status.
   *
   * @return The response status.
   */
  Status status();

  /**
   * Returns the response error if the response status is {@code Status.ERROR}
   *
   * @return The response error.
   */
  Error error();

  /**
   * Response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  interface Builder<T extends Builder<T, U>, U extends ProtocolResponse> extends io.atomix.copycat.util.Builder<U> {

    /**
     * Sets the response status.
     *
     * @param status The response status.
     * @return The response builder.
     * @throws NullPointerException if {@code status} is null
     */
    T withStatus(Status status);

    /**
     * Sets the response error type with a default message.
     *
     * @param type The response error type.
     * @return The response builder.
     * @throws NullPointerException if {@code type} is null
     */
    default T withError(Error.Type type) {
      return withError(type, null);
    }

    /**
     * Sets the response error.
     *
     * @param error The response error.
     * @return The response builder.
     */
    @SuppressWarnings("unchecked")
    default T withError(Error error) {
      if (error != null) {
        return withError(error.type(), error.message());
      }
      return (T) this;
    }

    /**
     * Sets the response error.
     *
     * @param type The response error type.
     * @param message The response error message.
     * @return The response builder.
     * @throws NullPointerException if {@code type} is null
     */
    T withError(Error.Type type, String message);

    /**
     * Creates a copy of the given response.
     *
     * @param response The response to copy.
     * @return The copied response.
     */
    U copy(U response);
  }
}
