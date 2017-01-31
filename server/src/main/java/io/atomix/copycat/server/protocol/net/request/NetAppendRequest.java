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

import io.atomix.copycat.server.protocol.request.AppendRequest;
import io.atomix.copycat.server.storage.Indexed;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

import java.util.ArrayList;
import java.util.List;

/**
 * TCP append request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetAppendRequest extends AppendRequest implements RaftNetRequest<NetAppendRequest> {
  private final long id;

  public NetAppendRequest(long id, long term, int leader, long logIndex, long logTerm, List<Indexed<? extends Entry<?>>> entries, long commitIndex, long globalIndex) {
    super(term, leader, logIndex, logTerm, entries, commitIndex, globalIndex);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.APPEND;
  }

  /**
   * TCP append request builder.
   */
  public static class Builder extends AppendRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public AppendRequest copy(AppendRequest request) {
      return new NetAppendRequest(id, request.term(), request.leader(), request.logIndex(), request.logTerm(), request.entries(), request.commitIndex(), request.globalIndex());
    }

    @Override
    public AppendRequest build() {
      return new NetAppendRequest(id, term, leader, logIndex, logTerm, entries, commitIndex, globalIndex);
    }
  }

  /**
   * Append request serializer.
   */
  public static class Serializer extends RaftNetRequest.Serializer<NetAppendRequest> {
    @Override
    @SuppressWarnings("unchecked")
    public void writeObject(BufferOutput output, NetAppendRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.term);
      output.writeInt(request.leader);
      output.writeLong(request.logIndex);
      output.writeLong(request.logTerm);
      output.writeInt(request.entries.size());
      for (Indexed entry : request.entries) {
        new Indexed.Serializer().writeObject(output, entry);
      }
      output.writeLong(request.commitIndex);
      output.writeLong(request.globalIndex);
    }

    @Override
    @SuppressWarnings("unchecked")
    public NetAppendRequest readObject(BufferInput input, Class<NetAppendRequest> type) {
      final long id = input.readLong();
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
      return new NetAppendRequest(id, term, leader, logIndex, logTerm, entries, commitIndex, globalIndex);
    }
  }
}
