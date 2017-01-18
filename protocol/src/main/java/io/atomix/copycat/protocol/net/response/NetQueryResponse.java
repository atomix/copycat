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
package io.atomix.copycat.protocol.net.response;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.response.QueryResponse;

/**
 * TCP query response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetQueryResponse extends QueryResponse implements NetResponse {
  private final long id;

  public NetQueryResponse(long id, Status status, CopycatError error, long index, long eventIndex, Object result) {
    super(status, error, index, eventIndex, result);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Types.QUERY_RESPONSE;
  }

  /**
   * TCP query response builder.
   */
  public static class Builder extends QueryResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public QueryResponse build() {
      return new NetQueryResponse(id, status, error, index, eventIndex, result);
    }
  }

  /**
   * Query response serializer.
   */
  public static class Serializer extends NetResponse.Serializer<NetQueryResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetQueryResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.error == null) {
        output.writeByte(0);
      } else {
        output.writeByte(response.error.id());
      }
      output.writeLong(response.index);
      output.writeLong(response.eventIndex);
      kryo.writeClassAndObject(output, response.result);
    }

    @Override
    public NetQueryResponse read(Kryo kryo, Input input, Class<NetQueryResponse> type) {
      return new NetQueryResponse(input.readLong(), Status.forId(input.readByte()), CopycatError.forId(input.readByte()), input.readLong(), input.readLong(), kryo.readClassAndObject(input));
    }
  }
}
