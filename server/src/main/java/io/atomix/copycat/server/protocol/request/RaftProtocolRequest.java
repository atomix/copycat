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
package io.atomix.copycat.server.protocol.request;

import io.atomix.copycat.protocol.request.ProtocolRequest;

/**
 * Raft protocol request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface RaftProtocolRequest extends ProtocolRequest {

  /**
   * Raft TCP request type.
   */
  class Type extends ProtocolRequest.Type {
    public static final Type JOIN        = new Type(0x07, JoinRequest.class);
    public static final Type LEAVE       = new Type(0x08, LeaveRequest.class);
    public static final Type INSTALL     = new Type(0x09, InstallRequest.class);
    public static final Type CONFIGURE   = new Type(0x0a, ConfigureRequest.class);
    public static final Type RECONFIGURE = new Type(0x0b, ReconfigureRequest.class);
    public static final Type ACCEPT      = new Type(0x0c, AcceptRequest.class);
    public static final Type POLL        = new Type(0x0d, PollRequest.class);
    public static final Type VOTE        = new Type(0x0e, VoteRequest.class);
    public static final Type APPEND      = new Type(0x0f, AppendRequest.class);

    protected Type(int id, Class<?> type) {
      super(id, type);
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
        case 0x07:
        case 0x08:
        case 0x09:
        case 0x0a:
        case 0x0b:
        case 0x0c:
        case 0x0d:
        case 0x0e:
        case 0x0f:
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
    public static ProtocolRequest.Type forId(int id) {
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
        case 0x07:
          return JOIN;
        case 0x08:
          return LEAVE;
        case 0x09:
          return INSTALL;
        case 0x0a:
          return CONFIGURE;
        case 0x0b:
          return RECONFIGURE;
        case 0x0c:
          return ACCEPT;
        case 0x0d:
          return POLL;
        case 0x0e:
          return VOTE;
        case 0x0f:
          return APPEND;
        default:
          throw new IllegalArgumentException("Unknown request type: " + id);
      }
    }
  }
}
