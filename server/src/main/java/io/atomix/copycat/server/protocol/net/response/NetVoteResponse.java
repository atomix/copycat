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

import io.atomix.copycat.protocol.net.response.NetResponse;
import io.atomix.copycat.protocol.response.AbstractResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.server.protocol.response.VoteResponse;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

/**
 * TCP vote response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetVoteResponse extends VoteResponse implements RaftNetResponse<NetVoteResponse> {
  private final long id;

  public NetVoteResponse(long id, ProtocolResponse.Status status, ProtocolResponse.Error error, long term, boolean voted) {
    super(status, error, term, voted);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.VOTE;
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
    public void writeObject(BufferOutput output, NetVoteResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.status == Status.OK) {
        output.writeLong(response.term);
        output.writeBoolean(response.voted);
      } else {
        output.writeByte(response.error.type().id());
        output.writeString(response.error.message());
      }
    }

    @Override
    public NetVoteResponse readObject(BufferInput input, Class<NetVoteResponse> type) {
      final long id = input.readLong();
      final Status status = Status.forId(input.readByte());
      if (status == Status.OK) {
        return new NetVoteResponse(id, status, null, input.readLong(), input.readBoolean());
      } else {
        NetResponse.Error error = new AbstractResponse.Error(ProtocolResponse.Error.Type.forId(input.readByte()), input.readString());
        return new NetVoteResponse(id, status, error, input.readLong(), input.readBoolean());
      }
    }
  }
}
