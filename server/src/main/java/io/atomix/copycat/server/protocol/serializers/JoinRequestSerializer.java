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
import io.atomix.copycat.server.protocol.request.JoinRequest;
import io.atomix.copycat.server.state.ServerMember;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

import java.time.Instant;

public class JoinRequestSerializer extends RaftProtocolRequestSerializer<JoinRequest> {
  @Override
  public void writeObject(BufferOutput output, JoinRequest request) {
    output.writeByte(request.member().type().ordinal());
    output.writeByte(request.member().status().ordinal());
    output.writeString(request.member().serverAddress().host()).writeInt(request.member().serverAddress().port());
    if (request.member().clientAddress() != null) {
      output.writeBoolean(true)
        .writeString(request.member().clientAddress().host())
        .writeInt(request.member().clientAddress().port());
    } else {
      output.writeBoolean(false);
    }
    output.writeLong(request.member().updated().toEpochMilli());
  }

  @Override
  public JoinRequest readObject(BufferInput input, Class<JoinRequest> type) {
    final Member.Type memberType = Member.Type.values()[input.readByte()];
    final Member.Status memberStatus = Member.Status.values()[input.readByte()];
    final Address serverAddress = new Address(input.readString(), input.readInt());
    Address clientAddress = null;
    if (input.readBoolean()) {
      clientAddress = new Address(input.readString(), input.readInt());
    }
    final Instant updated = Instant.ofEpochMilli(input.readLong());
    final Member member = new ServerMember(memberType, memberStatus, serverAddress, clientAddress, updated);
    return new JoinRequest(member);
  }
}