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

import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.net.response.NetResponse;
import io.atomix.copycat.protocol.response.AbstractResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.response.LeaveResponse;
import io.atomix.copycat.server.state.ServerMember;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * TCP leave response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetLeaveResponse extends LeaveResponse implements RaftNetResponse<NetLeaveResponse> {
  private final long id;

  public NetLeaveResponse(long id, ProtocolResponse.Status status, ProtocolResponse.Error error, long index, long term, long timestamp, Collection<Member> members) {
    super(status, error, index, term, timestamp, members);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.LEAVE;
  }

  /**
   * TCP leave response builder.
   */
  public static class Builder extends LeaveResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public LeaveResponse copy(LeaveResponse response) {
      return new NetLeaveResponse(id, response.status(), response.error(), response.index(), response.term(), response.timestamp(), response.members());
    }

    @Override
    public LeaveResponse build() {
      return new NetLeaveResponse(id, status, error, index, term, timestamp, members);
    }
  }

  /**
   * Leave response serializer.
   */
  public static class Serializer extends RaftNetResponse.Serializer<NetLeaveResponse> {
    @Override
    public void writeObject(BufferOutput output, NetLeaveResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.status == Status.OK) {
        output.writeLong(response.index);
        output.writeLong(response.term);
        output.writeLong(response.timestamp);
        output.writeInt(response.members.size());
        for (Member member : response.members) {
          output.writeByte(member.type().ordinal());
          output.writeString(member.serverAddress().host()).writeInt(member.serverAddress().port());
          if (member.clientAddress() != null) {
            output.writeBoolean(true)
              .writeString(member.clientAddress().host())
              .writeInt(member.clientAddress().port());
          } else {
            output.writeBoolean(false);
          }
          output.writeLong(member.updated().toEpochMilli());
        }
      } else {
        output.writeByte(response.error.type().id());
        output.writeString(response.error.message());
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public NetLeaveResponse readObject(BufferInput input, Class<NetLeaveResponse> type) {
      final long id = input.readLong();
      final Status status = Status.forId(input.readByte());
      if (status == Status.OK) {
        final long index = input.readLong();
        final long term = input.readLong();
        final long timestamp = input.readLong();
        final int size = input.readInt();
        final List<Member> members = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
          Member.Type memberType = Member.Type.values()[input.readByte()];
          Address serverAddress = new Address(input.readString(), input.readInt());
          Address clientAddress = null;
          if (input.readBoolean()) {
            clientAddress = new Address(input.readString(), input.readInt());
          }
          Instant updated = Instant.ofEpochMilli(input.readLong());
          members.add(new ServerMember(memberType, serverAddress, clientAddress, updated));
        }
        return new NetLeaveResponse(id, status, null, index, term, timestamp, members);
      } else {
        NetResponse.Error error = new AbstractResponse.Error(ProtocolResponse.Error.Type.forId(input.readByte()), input.readString());
        return new NetLeaveResponse(id, status, error, 0, 0, 0, null);
      }
    }
  }
}
