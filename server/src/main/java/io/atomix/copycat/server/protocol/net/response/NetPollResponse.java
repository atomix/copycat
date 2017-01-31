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
import io.atomix.copycat.server.protocol.response.PollResponse;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

/**
 * TCP poll response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetPollResponse extends PollResponse implements RaftNetResponse<NetPollResponse> {
  private final long id;

  public NetPollResponse(long id, ProtocolResponse.Status status, ProtocolResponse.Error error, long term, boolean accepted) {
    super(status, error, term, accepted);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.POLL;
  }

  /**
   * TCP poll response builder.
   */
  public static class Builder extends PollResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public PollResponse copy(PollResponse response) {
      return new NetPollResponse(id, response.status(), response.error(), response.term(), response.accepted());
    }

    @Override
    public PollResponse build() {
      return new NetPollResponse(id, status, error, term, accepted);
    }
  }

  /**
   * Poll response serializer.
   */
  public static class Serializer extends RaftNetResponse.Serializer<NetPollResponse> {
    @Override
    public void writeObject(BufferOutput output, NetPollResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.status == Status.OK) {
        output.writeLong(response.term);
        output.writeBoolean(response.accepted);
      } else {
        output.writeByte(response.error.type().id());
        output.writeString(response.error.message());
      }
    }

    @Override
    public NetPollResponse readObject(BufferInput input, Class<NetPollResponse> type) {
      final long id = input.readLong();
      final Status status = Status.forId(input.readByte());
      if (status == Status.OK) {
        return new NetPollResponse(id, status, null, input.readLong(), input.readBoolean());
      } else {
        NetResponse.Error error = new AbstractResponse.Error(ProtocolResponse.Error.Type.forId(input.readByte()), input.readString());
        return new NetPollResponse(id, status, error, input.readLong(), input.readBoolean());
      }
    }
  }
}
