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
import com.fasterxml.jackson.annotation.JsonGetter;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.response.ConnectResponse;

import java.util.Collection;
import java.util.Objects;

/**
 * Connect client response.
 * <p>
 * Connect responses are sent in response to a client establishing a new connection with a server.
 * Connect responses do not provide any additional metadata aside from whether or not the request
 * succeeded.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetConnectResponse extends AbstractNetResponse implements ConnectResponse {
  private final Address leader;
  private final Collection<Address> members;

  protected NetConnectResponse(long id, Status status, CopycatError error, Address leader, Collection<Address> members) {
    super(id, status, error);
    this.leader = leader;
    this.members = members;
  }

  @Override
  public Type type() {
    return Types.CONNECT_RESPONSE;
  }

  @Override
  @JsonGetter("leader")
  public Address leader() {
    return leader;
  }

  @Override
  @JsonGetter("members")
  public Collection<Address> members() {
    return members;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, leader, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NetConnectResponse) {
      NetConnectResponse response = (NetConnectResponse) object;
      return response.status == status
        && ((response.leader == null && leader == null)
        || (response.leader != null && leader != null && response.leader.equals(leader)))
        && ((response.members == null && members == null)
        || (response.members != null && members != null && response.members.equals(members)));
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, leader=%s, members=%s]", getClass().getSimpleName(), status, leader, members);
  }

  /**
   * Connect response builder.
   */
  public static class Builder extends AbstractNetResponse.Builder<ConnectResponse.Builder, ConnectResponse> implements ConnectResponse.Builder {
    private Address leader;
    private Collection<Address> members;

    public Builder(long id) {
      super(id);
    }

    @Override
    public Builder withLeader(Address leader) {
      this.leader = leader;
      return this;
    }

    @Override
    public Builder withMembers(Collection<Address> members) {
      this.members = Assert.notNull(members, "members");
      return this;
    }

    @Override
    public ConnectResponse build() {
      return new NetConnectResponse(id, status, error, leader, members);
    }
  }

  /**
   * Connect response serializer.
   */
  public static class Serializer extends AbstractNetResponse.Serializer<NetConnectResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetConnectResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.error == null) {
        output.writeByte(0);
      } else {
        output.writeByte(response.error.id());
      }
      kryo.writeObject(output, response.leader);
      kryo.writeObject(output, response.members);
    }

    @Override
    public NetConnectResponse read(Kryo kryo, Input input, Class<NetConnectResponse> type) {
      return new NetConnectResponse(input.readLong(), Status.forId(input.readByte()), CopycatError.forId(input.readByte()), kryo.readObject(input, Address.class), kryo.readObject(input, Collection.class));
    }
  }
}
