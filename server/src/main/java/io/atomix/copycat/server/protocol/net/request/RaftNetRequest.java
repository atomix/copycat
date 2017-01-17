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
package io.atomix.copycat.server.protocol.net.request;

import io.atomix.copycat.protocol.net.request.*;
import io.atomix.copycat.server.protocol.request.RaftProtocolRequest;

/**
 * Raft TCP request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface RaftNetRequest extends NetRequest, RaftProtocolRequest {

  /**
   * Protocol request type.
   */
  enum Types implements Type {
    CONNECT_REQUEST(0x00, NetConnectRequest.class),
    REGISTER_REQUEST(0x01, NetRegisterRequest.class),
    KEEP_ALIVE_REQUEST(0x02, NetKeepAliveRequest.class),
    UNREGISTER_REQUEST(0x03, NetUnregisterRequest.class),
    QUERY_REQUEST(0x04, NetQueryRequest.class),
    COMMAND_REQUEST(0x05, NetCommandRequest.class),
    PUBLISH_REQUEST(0x06, NetPublishRequest.class),
    JOIN_REQUEST(0x07, NetJoinRequest.class),
    LEAVE_REQUEST(0x08, NetLeaveRequest.class),
    INSTALL_REQUEST(0x09, NetInstallRequest.class),
    CONFIGURE_REQUEST(0x0a, NetConfigureRequest.class),
    RECONFIGURE_REQUEST(0x0b, NetReconfigureRequest.class),
    ACCEPT_REQUEST(0x0c, NetAcceptRequest.class),
    POLL_REQUEST(0x0d, NetPollRequest.class),
    VOTE_REQUEST(0x0e, NetVoteRequest.class),
    APPEND_REQUEST(0x0f, NetAppendRequest.class);

    private final int id;
    private final Class<? extends NetRequest> type;

    Types(int id, Class<? extends NetRequest> type) {
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
    public Class<? extends NetRequest> type() {
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
        case 0x07:
          return JOIN_REQUEST;
        case 0x08:
          return LEAVE_REQUEST;
        case 0x09:
          return INSTALL_REQUEST;
        case 0x0a:
          return CONFIGURE_REQUEST;
        case 0x0b:
          return RECONFIGURE_REQUEST;
        case 0x0c:
          return ACCEPT_REQUEST;
        case 0x0d:
          return POLL_REQUEST;
        case 0x0e:
          return VOTE_REQUEST;
        case 0x0f:
          return APPEND_REQUEST;
        default:
          throw new IllegalArgumentException("Unknown request type: " + id);
      }
    }
  }
}
