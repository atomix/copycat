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

import io.atomix.copycat.server.protocol.request.AppendRequest;
import io.atomix.copycat.server.storage.Indexed;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

import java.util.ArrayList;
import java.util.List;

public class AppendRequestSerializer extends RaftProtocolRequestSerializer<AppendRequest> {
  @Override
  @SuppressWarnings("unchecked")
  public void writeObject(BufferOutput output, AppendRequest request) {
    output.writeLong(request.term());
    output.writeInt(request.leader());
    output.writeLong(request.logIndex());
    output.writeLong(request.logTerm());
    output.writeInt(request.entries().size());
    for (Indexed entry : request.entries()) {
      new Indexed.Serializer().writeObject(output, entry);
    }
    output.writeLong(request.commitIndex());
    output.writeLong(request.globalIndex());
  }

  @Override
  @SuppressWarnings("unchecked")
  public AppendRequest readObject(BufferInput input, Class<AppendRequest> type) {
    final long term = input.readLong();
    final int leader = input.readInt();
    final long logIndex = input.readLong();
    final long logTerm = input.readLong();
    final int size = input.readInt();
    final List<Indexed<? extends Entry<?>>> entries = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      entries.add(new Indexed.Serializer().readObject(input, Indexed.class));
    }
    final long commitIndex = input.readLong();
    final long globalIndex = input.readLong();
    return new AppendRequest(term, leader, logIndex, logTerm, entries, commitIndex, globalIndex);
  }
}