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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.Query;
import io.atomix.copycat.protocol.request.QueryRequest;

/**
 * TCP query request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetQueryRequest extends QueryRequest implements NetRequest<NetQueryRequest> {
  private final long id;

  public NetQueryRequest(long id, long session, long sequence, long index, Query query) {
    super(session, sequence, index, query);
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
      return new NetQueryRequest(id, request.session(), request.sequence(), request.index(), request.query());
    }

    @Override
    public QueryRequest build() {
      return new NetQueryRequest(id, session, sequence, index, query);
    }
  }

  /**
   * Query request serializer.
   */
  public static class Serializer extends NetRequest.Serializer<NetQueryRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetQueryRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.session);
      output.writeLong(request.sequence);
      output.writeLong(request.index);
      kryo.writeClassAndObject(output, request.query);
    }

    @Override
    public NetQueryRequest read(Kryo kryo, Input input, Class<NetQueryRequest> type) {
      return new NetQueryRequest(input.readLong(), input.readLong(), input.readLong(), input.readLong(), (Query) kryo.readClassAndObject(input));
    }
  }
}
