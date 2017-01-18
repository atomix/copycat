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
import io.atomix.copycat.server.protocol.response.VoteResponse;

/**
 * TCP vote response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetVoteResponse extends VoteResponse implements RaftNetResponse {
  private final long id;

  public NetVoteResponse(long id, ProtocolResponse.Status status, CopycatError error, long term, boolean voted) {
    super(status, error, term, voted);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Types.VOTE_RESPONSE;
  }

  /**
   * TCP vote response builder.
   */
  public static class Builder extends VoteResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public VoteResponse copy(VoteResponse response) {
      return new NetVoteResponse(id, response.status(), response.error(), response.term(), response.voted());
    }

    @Override
    public VoteResponse build() {
      return new NetVoteResponse(id, status, error, term, voted);
    }
  }

  /**
   * Vote response serializer.
   */
  public static class Serializer extends RaftNetResponse.Serializer<NetVoteResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetVoteResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.error == null) {
        output.writeByte(0);
      } else {
        output.writeByte(response.error.id());
      }
      output.writeLong(response.term);
      output.writeBoolean(response.voted);
    }

    @Override
    public NetVoteResponse read(Kryo kryo, Input input, Class<NetVoteResponse> type) {
      return new NetVoteResponse(input.readLong(), ProtocolResponse.Status.forId(input.readByte()), CopycatError.forId(input.readByte()), input.readLong(), input.readBoolean());
    }
  }
}
