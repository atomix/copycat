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
import io.atomix.copycat.protocol.response.AbstractResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.response.JoinResponse;
import io.atomix.copycat.server.state.ServerMember;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class JoinResponseSerializer extends RaftProtocolResponseSerializer<JoinResponse> {
  @Override
  public void writeObject(BufferOutput output, JoinResponse response) {
    output.writeByte(response.status().id());
    if (response.status() == ProtocolResponse.Status.OK) {
      output.writeLong(response.index());
      output.writeLong(response.term());
      output.writeLong(response.timestamp());
      output.writeInt(response.members().size());
      for (Member member : response.members()) {
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
      if (response.error() != null) {
        output.writeByte(response.error().type().id());
        output.writeString(response.error().message());
      } else {
        output.writeByte(0);
      }
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public JoinResponse readObject(BufferInput input, Class<JoinResponse> type) {
    final ProtocolResponse.Status status = ProtocolResponse.Status.forId(input.readByte());
    if (status == ProtocolResponse.Status.OK) {
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
      return new JoinResponse(status, null, index, term, timestamp, members);
    } else {
      int errorType = input.readByte();
      if (errorType != 0) {
        ProtocolResponse.Error error = new AbstractResponse.Error(ProtocolResponse.Error.Type.forId(errorType), input.readString());
        return new JoinResponse(status, error, 0, 0, 0, null);
      } else {
        return new JoinResponse(status, null, 0, 0, 0, null);
      }
    }
  }
}