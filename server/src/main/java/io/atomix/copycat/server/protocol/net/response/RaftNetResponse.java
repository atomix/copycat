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

import com.esotericsoftware.kryo.Serializer;
import io.atomix.copycat.protocol.net.response.*;
import io.atomix.copycat.server.protocol.response.RaftProtocolResponse;

import java.util.function.Supplier;

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
    CONNECT_RESPONSE(0x10, NetConnectResponse.class, NetConnectResponse.Serializer::new),
    REGISTER_RESPONSE(0x11, NetRegisterResponse.class, NetRegisterResponse.Serializer::new),
    KEEP_ALIVE_RESPONSE(0x12, NetKeepAliveResponse.class, NetKeepAliveResponse.Serializer::new),
    UNREGISTER_RESPONSE(0x13, NetUnregisterResponse.class, NetUnregisterResponse.Serializer::new),
    QUERY_RESPONSE(0x14, NetQueryResponse.class, NetQueryResponse.Serializer::new),
    COMMAND_RESPONSE(0x15, NetCommandResponse.class, NetCommandResponse.Serializer::new),
    PUBLISH_RESPONSE(0x16, NetPublishResponse.class, NetPublishResponse.Serializer::new),
    JOIN_RESPONSE(0x17, NetJoinResponse.class, NetJoinResponse.Serializer::new),
    LEAVE_RESPONSE(0x18, NetLeaveResponse.class, NetLeaveResponse.Serializer::new),
    INSTALL_RESPONSE(0x19, NetInstallResponse.class, NetInstallResponse.Serializer::new),
    CONFIGURE_RESPONSE(0x1a, NetConfigureResponse.class, NetConfigureResponse.Serializer::new),
    RECONFIGURE_RESPONSE(0x1b, NetReconfigureResponse.class, NetReconfigureResponse.Serializer::new),
    ACCEPT_RESPONSE(0x1c, NetAcceptResponse.class, NetAcceptResponse.Serializer::new),
    POLL_RESPONSE(0x1d, NetPollResponse.class, NetPollResponse.Serializer::new),
    VOTE_RESPONSE(0x1e, NetVoteResponse.class, NetVoteResponse.Serializer::new),
    APPEND_RESPONSE(0x1f, NetAppendResponse.class, NetAppendResponse.Serializer::new);

    private final int id;
    private final Class<? extends NetResponse> type;
    private final Supplier<Serializer<?>> serializer;

    Types(int id, Class<? extends NetResponse> type, Supplier<Serializer<?>> serializer) {
      this.id = id;
      this.type = type;
      this.serializer = serializer;
    }

    @Override
    public int id() {
      return id;
    }

    @Override
    public Class<? extends NetResponse> type() {
      return type;
    }

    @Override
    public Supplier<Serializer<?>> serializer() {
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
