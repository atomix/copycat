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
package io.atomix.copycat.protocol.net.request;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.protocol.request.KeepAliveRequest;

/**
 * TCP keep alive request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetKeepAliveRequest extends KeepAliveRequest implements NetRequest {
  private final long id;

  public NetKeepAliveRequest(long id, long session, long commandSequence, long eventIndex) {
    super(session, commandSequence, eventIndex);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Types.KEEP_ALIVE_REQUEST;
  }

  /**
   * TCP keep alive request builder.
   */
  public static class Builder extends KeepAliveRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public KeepAliveRequest build() {
      return new NetKeepAliveRequest(id, session, commandSequence, eventIndex);
    }
  }

  /**
   * Keep-alive request serializer.
   */
  public static class Serializer extends NetRequest.Serializer<NetKeepAliveRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetKeepAliveRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.session);
      output.writeLong(request.commandSequence);
      output.writeLong(request.eventIndex);
    }

    @Override
    public NetKeepAliveRequest read(Kryo kryo, Input input, Class<NetKeepAliveRequest> type) {
      return new NetKeepAliveRequest(input.readLong(), input.readLong(), input.readLong(), input.readLong());
    }
  }
}
