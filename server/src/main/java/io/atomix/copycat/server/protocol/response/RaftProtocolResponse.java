/*
 * Copyright 2017 the original author or authors.
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
package io.atomix.copycat.server.protocol.response;

import io.atomix.copycat.protocol.response.ProtocolResponse;

/**
 * Raft protocol response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface RaftProtocolResponse extends ProtocolResponse {

  /**
   * Raft protocol response type.
   */
  class Type extends ProtocolResponse.Type {
    public static final Type        JOIN = new Type(0x17, JoinResponse.class);
    public static final Type       LEAVE = new Type(0x18, LeaveResponse.class);
    public static final Type     INSTALL = new Type(0x19, InstallResponse.class);
    public static final Type   CONFIGURE = new Type(0x1a, ConfigureResponse.class);
    public static final Type RECONFIGURE = new Type(0x1b, ReconfigureResponse.class);
    public static final Type      ACCEPT = new Type(0x1c, AcceptResponse.class);
    public static final Type        POLL = new Type(0x1d, PollResponse.class);
    public static final Type        VOTE = new Type(0x1e, VoteResponse.class);
    public static final Type      APPEND = new Type(0x1f, AppendResponse.class);

    protected Type(int id, Class<?> type) {
      super(id, type);
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
        case 0x17:
        case 0x18:
        case 0x19:
        case 0x1a:
        case 0x1b:
        case 0x1c:
        case 0x1d:
        case 0x1e:
        case 0x1f:
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
    public static ProtocolResponse.Type forId(int id) {
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
        case 0x17:
          return JOIN;
        case 0x18:
          return LEAVE;
        case 0x19:
          return INSTALL;
        case 0x1a:
          return CONFIGURE;
        case 0x1b:
          return RECONFIGURE;
        case 0x1c:
          return ACCEPT;
        case 0x1d:
          return POLL;
        case 0x1e:
          return VOTE;
        case 0x1f:
          return APPEND;
        default:
          throw new IllegalArgumentException("Unknown response type: " + id);
      }
    }
  }
}
