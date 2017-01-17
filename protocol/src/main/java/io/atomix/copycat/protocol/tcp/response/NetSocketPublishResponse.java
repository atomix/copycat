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
package io.atomix.copycat.protocol.tcp.response;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.response.PublishResponse;

import java.util.Objects;

/**
 * Event publish response.
 * <p>
 * Publish responses are sent by clients to servers to indicate the last successful index for which
 * an event message was handled in proper sequence. If the client receives an event message out of
 * sequence, it should respond with the index of the last event it received in sequence. If an event
 * message is received in sequence, it should respond with the index of that event. Once a client has
 * responded successfully to an event message, it will be removed from memory on the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetSocketPublishResponse extends NetSocketSessionResponse implements PublishResponse {
  private final long index;

  protected NetSocketPublishResponse(long id, Status status, CopycatError error, long index) {
    super(id, status, error);
    this.index = index;
  }

  @Override
  public Type type() {
    return Type.PUBLISH_RESPONSE;
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NetSocketPublishResponse) {
      NetSocketPublishResponse response = (NetSocketPublishResponse) object;
      return response.status == status
        && response.index == index;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, error=%s, index=%d]", getClass().getSimpleName(), status, error, index);
  }

  /**
   * Publish response builder.
   */
  public static class Builder extends NetSocketSessionResponse.Builder<PublishResponse.Builder, PublishResponse> implements PublishResponse.Builder {
    private long index;

    public Builder(long id) {
      super(id);
    }

    @Override
    public Builder withIndex(long index) {
      this.index = Assert.argNot(index, index < 0, "index cannot be less than 0");
      return this;
    }

    @Override
    public PublishResponse build() {
      return new NetSocketPublishResponse(id, status, error, index);
    }
  }

  /**
   * Publish response serializer.
   */
  public static class Serializer extends NetSocketSessionResponse.Serializer<NetSocketPublishResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetSocketPublishResponse response) {
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
    public NetSocketPublishResponse read(Kryo kryo, Input input, Class<NetSocketPublishResponse> type) {
      return new NetSocketPublishResponse(input.readLong(), Status.forId(input.readByte()), CopycatError.forId(input.readByte()), input.readLong());
    }
  }
}
