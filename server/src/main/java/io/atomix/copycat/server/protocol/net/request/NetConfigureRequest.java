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
import io.atomix.copycat.server.protocol.request.ConfigureRequest;
import io.atomix.copycat.server.state.ServerMember;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * TCP configure request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetConfigureRequest extends ConfigureRequest implements RaftNetRequest<NetConfigureRequest> {
  private final long id;

  public NetConfigureRequest(long id, long term, int leader, long index, long timestamp, Collection<Member> members) {
    super(term, leader, index, timestamp, members);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.CONFIGURE;
  }

  /**
   * TCP configure request builder.
   */
  public static class Builder extends ConfigureRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public ConfigureRequest copy(ConfigureRequest request) {
      return new NetConfigureRequest(id, request.term(), request.leader(), request.index(), request.timestamp(), request.members());
    }

    @Override
    public ConfigureRequest build() {
      return new NetConfigureRequest(id, term, leader, index, timestamp, members);
    }
  }

  /**
   * Configure request serializer.
   */
  public static class Serializer extends RaftNetRequest.Serializer<NetConfigureRequest> {
    @Override
    public void writeObject(BufferOutput output, NetConfigureRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.term);
      output.writeInt(request.leader);
      output.writeLong(request.index);
      output.writeLong(request.timestamp);
      output.writeInt(request.members.size());
      for (Member member : request.members) {
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
    }

    @Override
    @SuppressWarnings("unchecked")
    public NetConfigureRequest readObject(BufferInput input, Class<NetConfigureRequest> type) {
      final long id = input.readLong();
      final long term = input.readLong();
      final int leader = input.readInt();
      final long index = input.readLong();
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
      return new NetConfigureRequest(id, term, leader, index, timestamp, members);
    }
  }
}
