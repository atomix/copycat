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

import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.request.JoinRequest;
import io.atomix.copycat.server.state.ServerMember;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

import java.time.Instant;

/**
 * TCP join request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetJoinRequest extends JoinRequest implements RaftNetRequest<NetJoinRequest> {
  private final long id;

  public NetJoinRequest(long id, Member member) {
    super(member);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.JOIN;
  }

  /**
   * TCP join request builder.
   */
  public static class Builder extends JoinRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public JoinRequest copy(JoinRequest request) {
      return new NetJoinRequest(id, request.member());
    }

    @Override
    public JoinRequest build() {
      return new NetJoinRequest(id, member);
    }
  }

  /**
   * Join request serializer.
   */
  public static class Serializer extends RaftNetRequest.Serializer<NetJoinRequest> {
    @Override
    public void writeObject(BufferOutput output, NetJoinRequest request) {
      output.writeLong(request.id);
      output.writeByte(request.member.type().ordinal());
      output.writeString(request.member.serverAddress().host()).writeInt(request.member.serverAddress().port());
      if (request.member.clientAddress() != null) {
        output.writeBoolean(true)
          .writeString(request.member.clientAddress().host())
          .writeInt(request.member.clientAddress().port());
      } else {
        output.writeBoolean(false);
      }
      output.writeLong(request.member.updated().toEpochMilli());
    }

    @Override
    public NetJoinRequest readObject(BufferInput input, Class<NetJoinRequest> type) {
      final long id = input.readLong();
      final Member.Type memberType = Member.Type.values()[input.readByte()];
      final Address serverAddress = new Address(input.readString(), input.readInt());
      Address clientAddress = null;
      if (input.readBoolean()) {
        clientAddress = new Address(input.readString(), input.readInt());
      }
      final Instant updated = Instant.ofEpochMilli(input.readLong());
      final Member member = new ServerMember(memberType, serverAddress, clientAddress, updated);
      return new NetJoinRequest(id, member);
    }
  }
}
