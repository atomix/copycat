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
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.response.UnregisterResponse;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

/**
 * TCP unregister response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetUnregisterResponse extends UnregisterResponse implements NetResponse<NetUnregisterResponse> {
  private final long id;

  public NetUnregisterResponse(long id, Status status, ProtocolResponse.Error error) {
    super(status, error);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.UNREGISTER;
  }

  /**
   * TCP unregister response builder.
   */
  public static class Builder extends UnregisterResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public UnregisterResponse copy(UnregisterResponse response) {
      return new NetUnregisterResponse(id, response.status(), response.error());
    }

    @Override
    public UnregisterResponse build() {
      return new NetUnregisterResponse(id, status, error);
    }
  }

  /**
   * Unregister response serializer.
   */
  public static class Serializer extends NetResponse.Serializer<NetUnregisterResponse> {
    @Override
    public void writeObject(BufferOutput output, NetUnregisterResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.status == Status.ERROR) {
        output.writeByte(response.error.type().id());
        output.writeString(response.error.message());
      }
    }

    @Override
    public NetUnregisterResponse readObject(BufferInput input, Class<NetUnregisterResponse> type) {
      final long id = input.readLong();
      final Status status = Status.forId(input.readByte());
      if (status == Status.OK) {
        return new NetUnregisterResponse(id, status, null);
      } else {
        NetResponse.Error error = new AbstractResponse.Error(ProtocolResponse.Error.Type.forId(input.readByte()), input.readString());
        return new NetUnregisterResponse(id, status, error);
      }
    }
  }
}
