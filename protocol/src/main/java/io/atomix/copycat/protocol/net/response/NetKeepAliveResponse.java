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
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.response.KeepAliveResponse;
import io.atomix.copycat.protocol.websocket.request.WebSocketKeepAliveRequest;

import java.util.Collection;
import java.util.Objects;

/**
 * Session keep alive response.
 * <p>
 * Session keep alive responses are sent upon the completion of a {@link WebSocketKeepAliveRequest}
 * from a client. Keep alive responses, when successful, provide the current cluster configuration and leader
 * to the client to ensure clients can evolve with the structure of the cluster and make intelligent decisions
 * about connecting to the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetKeepAliveResponse extends NetSessionResponse implements KeepAliveResponse {
  private final Address leader;
  private final Collection<Address> members;

  protected NetKeepAliveResponse(long id, Status status, CopycatError error, Address leader, Collection<Address> members) {
    super(id, status, error);
    this.leader = leader;
    this.members = members;
  }

  @Override
  public Type type() {
    return Types.KEEP_ALIVE_RESPONSE;
  }

  @Override
  public Address leader() {
    return leader;
  }

  @Override
  public Collection<Address> members() {
    return members;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, leader, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NetKeepAliveResponse) {
      NetKeepAliveResponse response = (NetKeepAliveResponse) object;
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
    return String.format("%s[status=%s, error=%s, leader=%s, members=%s]", getClass().getSimpleName(), status, error, leader, members);
  }

  /**
   * Status response builder.
   */
  public static class Builder extends NetSessionResponse.Builder<KeepAliveResponse.Builder, KeepAliveResponse> implements KeepAliveResponse.Builder {
    private Address leader;
    private Collection<Address> members;

    public Builder(long id) {
      super(id);
    }

    /**
     * Sets the response leader.
     *
     * @param leader The response leader.
     * @return The response builder.
     */
    public Builder withLeader(Address leader) {
      this.leader = leader;
      return this;
    }

    /**
     * Sets the response members.
     *
     * @param members The response members.
     * @return The response builder.
     * @throws NullPointerException if {@code members} is null
     */
    public Builder withMembers(Collection<Address> members) {
      this.members = Assert.notNull(members, "members");
      return this;
    }

    /**
     * @throws IllegalStateException if status is OK and members is null
     */
    @Override
    public KeepAliveResponse build() {
      return new NetKeepAliveResponse(id, status, error, leader, members);
    }
  }

  /**
   * Keep-alive response serializer.
   */
  public static class Serializer extends NetSessionResponse.Serializer<NetKeepAliveResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetKeepAliveResponse response) {
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
    @SuppressWarnings("unchecked")
    public NetKeepAliveResponse read(Kryo kryo, Input input, Class<NetKeepAliveResponse> type) {
      return new NetKeepAliveResponse(input.readLong(), Status.forId(input.readByte()), CopycatError.forId(input.readByte()), kryo.readObject(input, Address.class), kryo.readObject(input, Collection.class));
    }
  }
}
