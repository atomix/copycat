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

import io.atomix.copycat.protocol.response.AbstractResponse;
import io.atomix.copycat.protocol.response.CommandResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

/**
 * TCP command response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetCommandResponse extends CommandResponse implements NetResponse<NetCommandResponse> {
  private final long id;

  public NetCommandResponse(long id, Status status, ProtocolResponse.Error error, long index, long eventIndex, byte[] result) {
    super(status, error, index, eventIndex, result);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.COMMAND;
  }

  /**
   * TCP command response builder.
   */
  public static class Builder extends CommandResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public CommandResponse copy(CommandResponse response) {
      return new NetCommandResponse(id, response.status(), response.error(), response.index(), response.eventIndex(), response.result());
    }

    @Override
    public CommandResponse build() {
      return new NetCommandResponse(id, status, error, index, eventIndex, result);
    }
  }

  /**
   * Command response serializer.
   */
  public static class Serializer extends NetResponse.Serializer<NetCommandResponse> {
    @Override
    public void writeObject(BufferOutput output, NetCommandResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.status == Status.ERROR) {
        output.writeByte(response.error.type().id());
        output.writeString(response.error.message());
      }
      output.writeLong(response.index);
      output.writeLong(response.eventIndex);
      output.writeInt(response.result.length);
      output.write(response.result);
    }

    @Override
    public NetCommandResponse readObject(BufferInput input, Class<NetCommandResponse> type) {
      final long id = input.readLong();
      final Status status = Status.forId(input.readByte());
      NetResponse.Error error = null;
      if (status == Status.ERROR) {
        error = new AbstractResponse.Error(ProtocolResponse.Error.Type.forId(input.readByte()), input.readString());
      }
      return new NetCommandResponse(id, status, error, input.readLong(), input.readLong(), input.readBytes(input.readInt()));
    }
  }
}
