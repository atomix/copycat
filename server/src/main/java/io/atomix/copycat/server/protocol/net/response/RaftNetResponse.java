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
package io.atomix.copycat.server.protocol.net.response;

import io.atomix.copycat.protocol.net.response.*;
import io.atomix.copycat.server.protocol.response.RaftProtocolResponse;

/**
 * Raft TCP response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface RaftNetResponse extends NetResponse, RaftProtocolResponse {

  /**
   * Protocol response type.
   */
  enum Types implements Type {
    CONNECT_RESPONSE(0x10, NetConnectResponse.class),
    REGISTER_RESPONSE(0x11, NetRegisterResponse.class),
    KEEP_ALIVE_RESPONSE(0x12, NetKeepAliveResponse.class),
    UNREGISTER_RESPONSE(0x13, NetUnregisterResponse.class),
    QUERY_RESPONSE(0x14, NetQueryResponse.class),
    COMMAND_RESPONSE(0x15, NetCommandResponse.class),
    PUBLISH_RESPONSE(0x16, NetPublishResponse.class),
    JOIN_RESPONSE(0x17, NetJoinResponse.class),
    LEAVE_RESPONSE(0x18, NetLeaveResponse.class),
    INSTALL_RESPONSE(0x19, NetInstallResponse.class),
    CONFIGURE_RESPONSE(0x1a, NetConfigureResponse.class),
    RECONFIGURE_RESPONSE(0x1b, NetReconfigureResponse.class),
    ACCEPT_RESPONSE(0x1c, NetAcceptResponse.class),
    POLL_RESPONSE(0x1d, NetPollResponse.class),
    VOTE_RESPONSE(0x1e, NetVoteResponse.class),
    APPEND_RESPONSE(0x1f, NetAppendResponse.class);

    private final int id;
    private final Class<? extends NetResponse> type;

    Types(int id, Class<? extends NetResponse> type) {
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
    public Class<? extends NetResponse> type() {
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
    public static Type forId(int id) {
      switch (id) {
        case 0x10:
          return CONNECT_RESPONSE;
        case 0x11:
          return REGISTER_RESPONSE;
        case 0x12:
          return KEEP_ALIVE_RESPONSE;
        case 0x13:
          return UNREGISTER_RESPONSE;
        case 0x14:
          return QUERY_RESPONSE;
        case 0x15:
          return COMMAND_RESPONSE;
        case 0x16:
          return PUBLISH_RESPONSE;
        case 0x17:
          return JOIN_RESPONSE;
        case 0x18:
          return LEAVE_RESPONSE;
        case 0x19:
          return INSTALL_RESPONSE;
        case 0x1a:
          return CONFIGURE_RESPONSE;
        case 0x1b:
          return RECONFIGURE_RESPONSE;
        case 0x1c:
          return ACCEPT_RESPONSE;
        case 0x1d:
          return POLL_RESPONSE;
        case 0x1e:
          return VOTE_RESPONSE;
        case 0x1f:
          return APPEND_RESPONSE;
        default:
          throw new IllegalArgumentException("Unknown response type: " + id);
      }
    }
  }
}
