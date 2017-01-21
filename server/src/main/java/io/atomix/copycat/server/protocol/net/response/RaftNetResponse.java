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

import io.atomix.copycat.protocol.net.response.NetResponse;

/**
 * Raft TCP response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface RaftNetResponse<T extends RaftNetResponse<T>> extends NetResponse<T> {

  /**
   * Raft TCP response type.
   */
  class Type<T extends RaftNetResponse<T>> extends NetResponse.Type<T> {
    public static final Type<NetJoinResponse>               JOIN = new Type<>(0x17, NetJoinResponse.class, new NetJoinResponse.Serializer());
    public static final Type<NetLeaveResponse>             LEAVE = new Type<>(0x18, NetLeaveResponse.class, new NetLeaveResponse.Serializer());
    public static final Type<NetInstallResponse>         INSTALL = new Type<>(0x19, NetInstallResponse.class, new NetInstallResponse.Serializer());
    public static final Type<NetConfigureResponse>     CONFIGURE = new Type<>(0x1a, NetConfigureResponse.class, new NetConfigureResponse.Serializer());
    public static final Type<NetReconfigureResponse> RECONFIGURE = new Type<>(0x1b, NetReconfigureResponse.class, new NetReconfigureResponse.Serializer());
    public static final Type<NetAcceptResponse>           ACCEPT = new Type<>(0x1c, NetAcceptResponse.class, new NetAcceptResponse.Serializer());
    public static final Type<NetPollResponse>               POLL = new Type<>(0x1d, NetPollResponse.class, new NetPollResponse.Serializer());
    public static final Type<NetVoteResponse>               VOTE = new Type<>(0x1e, NetVoteResponse.class, new NetVoteResponse.Serializer());
    public static final Type<NetAppendResponse>           APPEND = new Type<>(0x1f, NetAppendResponse.class, new NetAppendResponse.Serializer());

    protected Type(int id, Class<T> type, Serializer<T> serializer) {
      super(id, type, serializer);
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
    public static NetResponse.Type<?> forId(int id) {
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
