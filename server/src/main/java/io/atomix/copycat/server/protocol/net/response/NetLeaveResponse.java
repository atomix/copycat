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
package io.atomix.copycat.server.protocol.net.response;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.response.LeaveResponse;

import java.util.Collection;

/**
 * TCP leave response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetLeaveResponse extends LeaveResponse implements RaftNetResponse<NetLeaveResponse> {
  private final long id;

  public NetLeaveResponse(long id, ProtocolResponse.Status status, CopycatError error, long index, long term, long timestamp, Collection<Member> members) {
    super(status, error, index, term, timestamp, members);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.LEAVE;
  }

  /**
   * TCP leave response builder.
   */
  public static class Builder extends LeaveResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public LeaveResponse copy(LeaveResponse response) {
      return new NetLeaveResponse(id, response.status(), response.error(), response.index(), response.term(), response.timestamp(), response.members());
    }

    @Override
    public LeaveResponse build() {
      return new NetLeaveResponse(id, status, error, index, term, timestamp, members);
    }
  }

  /**
   * Leave response serializer.
   */
  public static class Serializer extends RaftNetResponse.Serializer<NetLeaveResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetLeaveResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.error == null) {
        output.writeByte(0);
      } else {
        output.writeByte(response.error.id());
      }
      output.writeLong(response.index);
      output.writeLong(response.term);
      output.writeLong(response.timestamp);
      kryo.writeObject(output, response.members);
    }

    @Override
    public NetLeaveResponse read(Kryo kryo, Input input, Class<NetLeaveResponse> type) {
      return new NetLeaveResponse(input.readLong(), ProtocolResponse.Status.forId(input.readByte()), CopycatError.forId(input.readByte()), input.readLong(), input.readLong(), input.readLong(), kryo.readObject(input, Collection.class));
    }
  }
}
