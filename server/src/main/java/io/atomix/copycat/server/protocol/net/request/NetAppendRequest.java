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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.server.protocol.request.AppendRequest;
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.List;

/**
 * TCP append request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetAppendRequest extends AppendRequest implements RaftNetRequest {
  private final long id;

  public NetAppendRequest(long id, long term, int leader, long logIndex, long logTerm, List<Entry> entries, long commitIndex, long globalIndex) {
    super(term, leader, logIndex, logTerm, entries, commitIndex, globalIndex);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Types.APPEND_REQUEST;
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
    public void write(Kryo kryo, Output output, NetAppendRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.term);
      output.writeInt(request.leader);
      output.writeLong(request.logIndex);
      output.writeLong(request.logTerm);
      kryo.writeObject(output, request.entries);
      output.writeLong(request.commitIndex);
      output.writeLong(request.globalIndex);
    }

    @Override
    @SuppressWarnings("unchecked")
    public NetAppendRequest read(Kryo kryo, Input input, Class<NetAppendRequest> type) {
      return new NetAppendRequest(input.readLong(), input.readLong(), input.readInt(), input.readLong(), input.readLong(), kryo.readObject(input, List.class), input.readLong(), input.readLong());
    }
  }
}
