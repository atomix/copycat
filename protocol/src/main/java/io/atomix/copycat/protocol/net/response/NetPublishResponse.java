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
import io.atomix.copycat.protocol.response.PublishResponse;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

/**
 * TCP publish response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetPublishResponse extends PublishResponse implements NetResponse<NetPublishResponse> {
  private final long id;

  public NetPublishResponse(long id, Status status, ProtocolResponse.Error error, long index) {
    super(status, error, index);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.PUBLISH;
  }

  /**
   * TCP publish response builder.
   */
  public static class Builder extends PublishResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public PublishResponse copy(PublishResponse response) {
      return new NetPublishResponse(id, response.status(), response.error(), response.index());
    }

    @Override
    public PublishResponse build() {
      return new NetPublishResponse(id, status, error, index);
    }
  }

  /**
   * Publish response serializer.
   */
  public static class Serializer extends NetResponse.Serializer<NetPublishResponse> {
    @Override
    public void writeObject(BufferOutput output, NetPublishResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.status == Status.ERROR) {
        output.writeByte(response.error.type().id());
        output.writeString(response.error.message());
      }
      output.writeLong(response.index);
    }

    @Override
    public NetPublishResponse readObject(BufferInput input, Class<NetPublishResponse> type) {
      final long id = input.readLong();
      final Status status = Status.forId(input.readByte());
      if (status == Status.OK) {
        return new NetPublishResponse(id, status, null, input.readLong());
      } else {
        NetResponse.Error error = new AbstractResponse.Error(ProtocolResponse.Error.Type.forId(input.readByte()), input.readString());
        return new NetPublishResponse(id, status, error, input.readLong());
      }
    }
  }
}
