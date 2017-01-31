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

import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.response.AbstractResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.response.RegisterResponse;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
    public void writeObject(BufferOutput output, NetRegisterResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.status == Status.OK) {
        output.writeLong(response.session);
        output.writeLong(response.timeout);
        output.writeString(response.leader.host()).writeInt(response.leader.port());
        output.writeInt(response.members.size());
        for (Address address : response.members) {
          output.writeString(address.host()).writeInt(address.port());
        }
      } else {
        output.writeByte(response.error.type().id());
        output.writeString(response.error.message());
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public NetRegisterResponse readObject(BufferInput input, Class<NetRegisterResponse> type) {
      final long id = input.readLong();
      final Status status = Status.forId(input.readByte());
      if (status == Status.OK) {
        final long session = input.readLong();
        final long timeout = input.readLong();
        final Address leader = new Address(input.readString(), input.readInt());
        final int size = input.readInt();
        final List<Address> members = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
          members.add(new Address(input.readString(), input.readInt()));
        }
        return new NetRegisterResponse(id, status, null, session, leader, members, timeout);
      } else {
        NetResponse.Error error = new AbstractResponse.Error(ProtocolResponse.Error.Type.forId(input.readByte()), input.readString());
        return new NetRegisterResponse(id, status, error, 0, null, null, 0);
      }
    }
  }
}
