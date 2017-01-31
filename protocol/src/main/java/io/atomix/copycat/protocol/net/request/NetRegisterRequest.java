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

import io.atomix.copycat.protocol.request.RegisterRequest;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

/**
 * TCP register request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetRegisterRequest extends RegisterRequest implements NetRequest<NetRegisterRequest> {
  private final long id;

  public NetRegisterRequest(long id, String client, long timeout) {
    super(client, timeout);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.REGISTER;
  }

  /**
   * TCP register request builder.
   */
  public static class Builder extends RegisterRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public RegisterRequest copy(RegisterRequest request) {
      return new NetRegisterRequest(id, request.client(), request.timeout());
    }

    @Override
    public RegisterRequest build() {
      return new NetRegisterRequest(id, client, timeout);
    }
  }

  /**
   * Register request serializer.
   */
  public static class Serializer extends NetRequest.Serializer<NetRegisterRequest> {
    @Override
    public void writeObject(BufferOutput output, NetRegisterRequest request) {
      output.writeLong(request.id);
      output.writeString(request.client);
      output.writeLong(request.timeout);
    }

    @Override
    public NetRegisterRequest readObject(BufferInput input, Class<NetRegisterRequest> type) {
      return new NetRegisterRequest(input.readLong(), input.readString(), input.readLong());
    }
  }
}
