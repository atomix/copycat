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
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.response.RegisterResponse;

import java.util.Collection;

/**
 * TCP register response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetRegisterResponse extends RegisterResponse implements NetResponse<NetRegisterResponse> {
  private final long id;

  public NetRegisterResponse(long id, Status status, ProtocolResponse.Error error, long session, Address leader, Collection<Address> members, long timeout) {
    super(status, error, session, leader, members, timeout);
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
   * TCP register response builder.
   */
  public static class Builder extends RegisterResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public RegisterResponse copy(RegisterResponse response) {
      return new NetRegisterResponse(id, response.status(), response.error(), response.session(), response.leader(), response.members(), response.timeout());
    }

    @Override
    public RegisterResponse build() {
      return new NetRegisterResponse(id, status, error, session, leader, members, timeout);
    }
  }

  /**
   * Register response serializer.
   */
  public static class Serializer extends NetResponse.Serializer<NetRegisterResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetRegisterResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.status == Status.OK) {
        output.writeLong(response.session);
        kryo.writeObject(output, response.leader);
        kryo.writeClassAndObject(output, response.members);
        output.writeLong(response.timeout);
      } else {
        output.writeByte(response.error.type().id());
        output.writeString(response.error.message());
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public NetRegisterResponse read(Kryo kryo, Input input, Class<NetRegisterResponse> type) {
      final long id = input.readLong();
      final Status status = Status.forId(input.readByte());
      if (status == Status.OK) {
        return new NetRegisterResponse(id, status, null, input.readLong(), kryo.readObject(input, Address.class), (Collection) kryo.readClassAndObject(input), input.readLong());
      } else {
        NetResponse.Error error = new AbstractResponse.Error(ProtocolResponse.Error.Type.forId(input.readByte()), input.readString());
        return new NetRegisterResponse(id, status, error, 0, null, null, 0);
      }
    }
  }
}
