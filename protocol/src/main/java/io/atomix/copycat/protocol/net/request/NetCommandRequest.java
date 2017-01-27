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
import io.atomix.copycat.protocol.request.CommandRequest;

/**
 * TCP command request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetCommandRequest extends CommandRequest implements NetRequest<NetCommandRequest> {
  private final long id;

  public NetCommandRequest(long id, long session, long sequence, byte[] bytes) {
    super(session, sequence, bytes);
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
   * TCP command request builder.
   */
  public static class Builder extends CommandRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public CommandRequest copy(CommandRequest request) {
      return new NetCommandRequest(id, request.session(), request.sequence(), request.bytes());
    }

    @Override
    public CommandRequest build() {
      return new NetCommandRequest(id, session, sequence, bytes);
    }
  }

  /**
   * Command request serializer.
   */
  public static class Serializer extends NetRequest.Serializer<NetCommandRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetCommandRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.session);
      output.writeLong(request.sequence);
      output.writeInt(request.bytes.length);
      output.write(request.bytes);
    }

    @Override
    public NetCommandRequest read(Kryo kryo, Input input, Class<NetCommandRequest> type) {
      return new NetCommandRequest(input.readLong(), input.readLong(), input.readLong(), input.readBytes(input.readInt()));
    }
  }
}
