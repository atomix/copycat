/*
 * Copyright 2015 the original author or authors.
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
 * limitations under the License.
 */
package io.atomix.copycat.protocol;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;

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
public class ConnectResponse extends AbstractResponse {

  /**
   * Returns a new connect client response builder.
   *
   * @return A new connect client response builder.
   */
  public static Builder builder() {
    return new Builder(new ConnectResponse());
  }

  /**
   * Returns a connect client response builder for an existing response.
   *
   * @param response The response to build.
   * @return The connect client response builder.
   * @throws NullPointerException if {@code response} is null
   */
  public static Builder builder(ConnectResponse response) {
    return new Builder(response);
  }

  private Address leader;
  private Collection<Address> members;

  /**
   * Returns the cluster leader.
   *
   * @return The cluster leader.
   */
  public Address leader() {
    return leader;
  }

  /**
   * Returns the cluster members.
   *
   * @return The cluster members.
   */
  public Collection<Address> members() {
    return members;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      leader = serializer.readObject(buffer);
      members = serializer.readObject(buffer);
    } else {
      error = CopycatError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      serializer.writeObject(leader, buffer);
      serializer.writeObject(members, buffer);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, leader, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ConnectResponse) {
      ConnectResponse response = (ConnectResponse) object;
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
   * Connect response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, ConnectResponse> {
    protected Builder(ConnectResponse response) {
      super(response);
    }

    /**
     * Sets the response leader.
     *
     * @param leader The response leader.
     * @return The response builder.
     */
    public Builder withLeader(Address leader) {
      response.leader = leader;
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
      response.members = Assert.notNull(members, "members");
      return this;
    }

    /**
     * @throws IllegalStateException if status is OK and members is null
     */
    @Override
    public ConnectResponse build() {
      super.build();
      Assert.stateNot(response.status == Status.OK && response.members == null, "members cannot be null");
      return response;
    }
  }

}
