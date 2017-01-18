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
import io.atomix.copycat.protocol.request.UnregisterRequest;

/**
 * TCP unregister request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetUnregisterRequest extends UnregisterRequest implements NetRequest {
  private final long id;

  public NetUnregisterRequest(long id, long session) {
    super(session);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Types.UNREGISTER_REQUEST;
  }

  /**
   * TCP unregister request builder.
   */
  public static class Builder extends UnregisterRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public UnregisterRequest copy(UnregisterRequest request) {
      return new NetUnregisterRequest(id, request.session());
    }

    @Override
    public UnregisterRequest build() {
      return new NetUnregisterRequest(id, session);
    }
  }

  /**
   * Unregister request serializer.
   */
  public static class Serializer extends NetRequest.Serializer<NetUnregisterRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetUnregisterRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.session);
    }

    @Override
    public NetUnregisterRequest read(Kryo kryo, Input input, Class<NetUnregisterRequest> type) {
      return new NetUnregisterRequest(input.readLong(), input.readLong());
    }
  }
}
