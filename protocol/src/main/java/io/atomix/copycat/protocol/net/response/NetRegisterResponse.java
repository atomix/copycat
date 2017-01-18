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
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.response.RegisterResponse;

import java.util.Collection;

/**
 * TCP register response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetRegisterResponse extends RegisterResponse implements NetResponse {
  private final long id;

  public NetRegisterResponse(long id, Status status, CopycatError error, long session, Address leader, Collection<Address> members, long timeout) {
    super(status, error, session, leader, members, timeout);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Types.REGISTER_RESPONSE;
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
      if (response.error == null) {
        output.writeByte(0);
      } else {
        output.writeByte(response.error.id());
      }
      output.writeLong(response.session);
      kryo.writeObject(output, response.leader);
      kryo.writeObject(output, response.members);
      output.writeLong(response.timeout);
    }

    @Override
    @SuppressWarnings("unchecked")
    public NetRegisterResponse read(Kryo kryo, Input input, Class<NetRegisterResponse> type) {
      return new NetRegisterResponse(input.readLong(), Status.forId(input.readByte()), CopycatError.forId(input.readByte()), input.readLong(), kryo.readObject(input, Address.class), kryo.readObject(input, Collection.class), input.readLong());
    }
  }
}
