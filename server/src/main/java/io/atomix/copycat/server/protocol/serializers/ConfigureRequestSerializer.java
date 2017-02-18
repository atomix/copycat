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
package io.atomix.copycat.server.protocol.serializers;

import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.request.ConfigureRequest;
import io.atomix.copycat.server.state.ServerMember;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class ConfigureRequestSerializer extends RaftProtocolRequestSerializer<ConfigureRequest> {
  @Override
  public void writeObject(BufferOutput output, ConfigureRequest request) {
    output.writeLong(request.term());
    output.writeInt(request.leader());
    output.writeLong(request.index());
    output.writeLong(request.timestamp());
    output.writeInt(request.members().size());
    for (Member member : request.members()) {
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
  public ConfigureRequest readObject(BufferInput input, Class<ConfigureRequest> type) {
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
    return new ConfigureRequest(term, leader, index, timestamp, members);
  }
}