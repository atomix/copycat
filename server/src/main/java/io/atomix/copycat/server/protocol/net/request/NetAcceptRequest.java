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
package io.atomix.copycat.server.protocol.net.request;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.server.protocol.request.AcceptRequest;

/**
 * TCP accept request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetAcceptRequest extends AcceptRequest implements RaftNetRequest {
  private final long id;

  public NetAcceptRequest(long id, String client, Address address) {
    super(client, address);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Types.ACCEPT_REQUEST;
  }

  /**
   * TCP accept request builder.
   */
  public static class Builder extends AcceptRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public AcceptRequest build() {
      return new NetAcceptRequest(id, client, address);
    }
  }

  /**
   * Accept request serializer.
   */
  public static class Serializer extends RaftNetRequest.Serializer<NetAcceptRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetAcceptRequest request) {
      output.writeLong(request.id);
      output.writeString(request.client);
      kryo.writeClassAndObject(output, request.address);
    }

    @Override
    public NetAcceptRequest read(Kryo kryo, Input input, Class<NetAcceptRequest> type) {
      return new NetAcceptRequest(input.readLong(), input.readString(), (Address) kryo.readClassAndObject(input));
    }
  }
}
