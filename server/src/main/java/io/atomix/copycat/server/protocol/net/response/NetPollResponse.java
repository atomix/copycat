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
import io.atomix.copycat.server.protocol.response.PollResponse;

/**
 * TCP poll response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetPollResponse extends PollResponse implements RaftNetResponse {
  private final long id;

  public NetPollResponse(long id, ProtocolResponse.Status status, CopycatError error, long term, boolean accepted) {
    super(status, error, term, accepted);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Types.POLL_RESPONSE;
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
    public void write(Kryo kryo, Output output, NetPollResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.error == null) {
        output.writeByte(0);
      } else {
        output.writeByte(response.error.id());
      }
      output.writeLong(response.term);
      output.writeBoolean(response.accepted);
    }

    @Override
    public NetPollResponse read(Kryo kryo, Input input, Class<NetPollResponse> type) {
      return new NetPollResponse(input.readLong(), ProtocolResponse.Status.forId(input.readByte()), CopycatError.forId(input.readByte()), input.readLong(), input.readBoolean());
    }
  }
}
