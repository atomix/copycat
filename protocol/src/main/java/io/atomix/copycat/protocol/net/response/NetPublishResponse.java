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
import io.atomix.copycat.protocol.response.PublishResponse;

/**
 * TCP publish response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetPublishResponse extends PublishResponse implements NetResponse {
  private final long id;

  public NetPublishResponse(long id, Status status, CopycatError error, long index) {
    super(status, error, index);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Types.PUBLISH_RESPONSE;
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
    public PublishResponse build() {
      return new NetPublishResponse(id, status, error, index);
    }
  }

  /**
   * Publish response serializer.
   */
  public static class Serializer extends NetResponse.Serializer<NetPublishResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetPublishResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.error == null) {
        output.writeByte(0);
      } else {
        output.writeByte(response.error.id());
      }
      output.writeLong(response.index);
    }

    @Override
    public NetPublishResponse read(Kryo kryo, Input input, Class<NetPublishResponse> type) {
      return new NetPublishResponse(input.readLong(), Status.forId(input.readByte()), CopycatError.forId(input.readByte()), input.readLong());
    }
  }
}
