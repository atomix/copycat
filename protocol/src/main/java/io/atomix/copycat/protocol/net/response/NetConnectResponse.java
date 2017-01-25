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
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.response.AbstractResponse;
import io.atomix.copycat.protocol.response.ConnectResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;

import java.util.Collection;

/**
 * TCP connect response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetConnectResponse extends ConnectResponse implements NetResponse<NetConnectResponse> {
  private final long id;

  public NetConnectResponse(long id, Status status, ProtocolResponse.Error error, Address leader, Collection<Address> members) {
    super(status, error, leader, members);
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
   * TCP connect response builder.
   */
  public static class Builder extends ConnectResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public ConnectResponse copy(ConnectResponse response) {
      return new NetConnectResponse(id, response.status(), response.error(), response.leader(), response.members());
    }

    @Override
    public ConnectResponse build() {
      return new NetConnectResponse(id, status, error, leader, members);
    }
  }

  /**
   * Connect response serializer.
   */
  public static class Serializer extends NetResponse.Serializer<NetConnectResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetConnectResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.status == Status.OK) {
        kryo.writeObject(output, response.leader);
        kryo.writeClassAndObject(output, response.members);
      } else {
        output.writeByte(response.error.type().id());
        output.writeString(response.error.message());
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public NetConnectResponse read(Kryo kryo, Input input, Class<NetConnectResponse> type) {
      final long id = input.readLong();
      final Status status = Status.forId(input.readByte());
      if (status == Status.OK) {
        return new NetConnectResponse(id, status, null, kryo.readObject(input, Address.class), (Collection) kryo.readClassAndObject(input));
      } else {
        NetResponse.Error error = new AbstractResponse.Error(ProtocolResponse.Error.Type.forId(input.readByte()), input.readString());
        return new NetConnectResponse(id, status, error, null, null);
      }
    }
  }
}
