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

import io.atomix.copycat.protocol.net.request.NetRequest;

/**
 * Raft TCP request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface RaftNetRequest<T extends RaftNetRequest<T>> extends NetRequest<T> {

  /**
   * Raft TCP request type.
   */
  class Type<T extends RaftNetRequest<T>> extends NetRequest.Type<T> {
    public static final Type<NetJoinRequest>               JOIN = new Type<>(0x07, NetJoinRequest.class, new NetJoinRequest.Serializer());
    public static final Type<NetLeaveRequest>             LEAVE = new Type<>(0x08, NetLeaveRequest.class, new NetLeaveRequest.Serializer());
    public static final Type<NetInstallRequest>         INSTALL = new Type<>(0x09, NetInstallRequest.class, new NetInstallRequest.Serializer());
    public static final Type<NetConfigureRequest>     CONFIGURE = new Type<>(0x0a, NetConfigureRequest.class, new NetConfigureRequest.Serializer());
    public static final Type<NetReconfigureRequest> RECONFIGURE = new Type<>(0x0b, NetReconfigureRequest.class, new NetReconfigureRequest.Serializer());
    public static final Type<NetAcceptRequest>           ACCEPT = new Type<>(0x0c, NetAcceptRequest.class, new NetAcceptRequest.Serializer());
    public static final Type<NetPollRequest>               POLL = new Type<>(0x0d, NetPollRequest.class, new NetPollRequest.Serializer());
    public static final Type<NetVoteRequest>               VOTE = new Type<>(0x0e, NetVoteRequest.class, new NetVoteRequest.Serializer());
    public static final Type<NetAppendRequest>           APPEND = new Type<>(0x0f, NetAppendRequest.class, new NetAppendRequest.Serializer());

    protected Type(int id, Class<T> type, Serializer<T> serializer) {
      super(id, type, serializer);
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
    public static NetRequest.Type<?> forId(int id) {
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
