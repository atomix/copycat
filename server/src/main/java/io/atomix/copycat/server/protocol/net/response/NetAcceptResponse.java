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
import io.atomix.copycat.server.protocol.response.AcceptResponse;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

/**
 * TCP accept response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetAcceptResponse extends AcceptResponse implements RaftNetResponse<NetAcceptResponse> {
  private final long id;

  public NetAcceptResponse(long id, ProtocolResponse.Status status, ProtocolResponse.Error error) {
    super(status, error);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.ACCEPT;
  }

  /**
   * TCP accept response builder.
   */
  public static class Builder extends AcceptResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public AcceptResponse copy(AcceptResponse response) {
      return new NetAcceptResponse(id, response.status(), response.error());
    }

    @Override
    public AcceptResponse build() {
      return new NetAcceptResponse(id, status, error);
    }
  }

  /**
   * Accept response serializer.
   */
  public static class Serializer extends RaftNetResponse.Serializer<NetAcceptResponse> {
    @Override
    public void writeObject(BufferOutput output, NetAcceptResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.status == Status.ERROR) {
        output.writeByte(response.error.type().id());
        output.writeString(response.error.message());
      }
    }

    @Override
    public NetAcceptResponse readObject(BufferInput input, Class<NetAcceptResponse> type) {
      final long id = input.readLong();
      final Status status = Status.forId(input.readByte());
      if (status == Status.OK) {
        return new NetAcceptResponse(id, status, null);
      } else {
        NetResponse.Error error = new AbstractResponse.Error(ProtocolResponse.Error.Type.forId(input.readByte()), input.readString());
        return new NetAcceptResponse(id, status, error);
      }
    }
  }
}
