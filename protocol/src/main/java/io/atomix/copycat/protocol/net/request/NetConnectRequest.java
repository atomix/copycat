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

import io.atomix.copycat.protocol.request.ConnectRequest;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

/**
 * TCP connect request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetConnectRequest extends ConnectRequest implements NetRequest<NetConnectRequest> {
  private final long id;

  public NetConnectRequest(long id, String client) {
    super(client);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.CONNECT;
  }

  /**
   * TCP connect request builder.
   */
  public static class Builder extends ConnectRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public ConnectRequest copy(ConnectRequest request) {
      return new NetConnectRequest(id, request.client());
    }

    @Override
    public ConnectRequest build() {
      return new NetConnectRequest(id, client);
    }
  }

  /**
   * Connect request serializer.
   */
  public static class Serializer extends NetRequest.Serializer<NetConnectRequest> {
    @Override
    public void writeObject(BufferOutput output, NetConnectRequest request) {
      output.writeLong(request.id);
      output.writeString(request.client);
    }

    @Override
    public NetConnectRequest readObject(BufferInput input, Class<NetConnectRequest> type) {
      return new NetConnectRequest(input.readLong(), input.readString());
    }
  }
}
