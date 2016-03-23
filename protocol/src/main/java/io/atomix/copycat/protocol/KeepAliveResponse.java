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
 * Session keep alive response.
 * <p>
 * Session keep alive responses are sent upon the completion of a {@link KeepAliveRequest}
 * from a client. Keep alive responses, when successful, provide the current cluster configuration and leader
 * to the client to ensure clients can evolve with the structure of the cluster and make intelligent decisions
 * about connecting to the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KeepAliveResponse extends SessionResponse {

  /**
   * Returns a new keep alive response builder.
   *
   * @return A new keep alive response builder.
   */
  public static Builder builder() {
    return new Builder(new KeepAliveResponse());
  }

  /**
   * Returns a keep alive response builder for an existing response.
   *
   * @param response The response to build.
   * @return The keep alive response builder.
   * @throws NullPointerException if {@code response} is null
   */
  public static Builder builder(KeepAliveResponse response) {
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
      leader = serializer.readObject(buffer);
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
      serializer.writeObject(leader, buffer);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, leader, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof KeepAliveResponse) {
      KeepAliveResponse response = (KeepAliveResponse) object;
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
  public static class Builder extends SessionResponse.Builder<Builder, KeepAliveResponse> {

    protected Builder(KeepAliveResponse response) {
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
    public KeepAliveResponse build() {
      super.build();
      Assert.stateNot(response.status == Status.OK && response.members == null, "members cannot be null");
      return response;
    }
  }

}
