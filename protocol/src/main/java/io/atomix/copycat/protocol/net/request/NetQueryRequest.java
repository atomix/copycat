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
package io.atomix.copycat.protocol.net.request;

import io.atomix.copycat.ConsistencyLevel;
import io.atomix.copycat.protocol.request.QueryRequest;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

/**
 * TCP query request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetQueryRequest extends QueryRequest implements NetRequest<NetQueryRequest> {
  private final long id;

  public NetQueryRequest(long id, long session, long sequence, long index, byte[] bytes, ConsistencyLevel consistency) {
    super(session, sequence, index, bytes, consistency);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.QUERY;
  }

  /**
   * TCP query request builder.
   */
  public static class Builder extends QueryRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public QueryRequest copy(QueryRequest request) {
      return new NetQueryRequest(id, request.session(), request.sequence(), request.index(), request.bytes(), request.consistency());
    }

    @Override
    public QueryRequest build() {
      return new NetQueryRequest(id, session, sequence, index, bytes, consistency);
    }
  }

  /**
   * Query request serializer.
   */
  public static class Serializer extends NetRequest.Serializer<NetQueryRequest> {
    @Override
    public void writeObject(BufferOutput output, NetQueryRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.session);
      output.writeLong(request.sequence);
      output.writeLong(request.index);
      output.writeInt(request.bytes.length);
      output.write(request.bytes);
      output.writeByte(request.consistency.id);
    }

    @Override
    public NetQueryRequest readObject(BufferInput input, Class<NetQueryRequest> type) {
      return new NetQueryRequest(input.readLong(), input.readLong(), input.readLong(), input.readLong(), input.readBytes(input.readInt()), ConsistencyLevel.forId(input.readByte()));
    }
  }
}
