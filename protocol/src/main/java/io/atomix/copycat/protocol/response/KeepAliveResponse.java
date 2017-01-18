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
package io.atomix.copycat.protocol.response;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.Address;
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
public class KeepAliveResponse extends SessionResponse {
  protected final Address leader;
  protected final Collection<Address> members;

  protected KeepAliveResponse(Status status, CopycatError error, Address leader, Collection<Address> members) {
    super(status, error);
    this.leader = leader;
    this.members = Assert.notNull(members, "members");
  }

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
  public static class Builder extends SessionResponse.Builder<KeepAliveResponse.Builder, KeepAliveResponse> {
    protected Address leader;
    protected Collection<Address> members;

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

    @Override
    public KeepAliveResponse copy(KeepAliveResponse response) {
      return new KeepAliveResponse(response.status, response.error, response.leader, response.members);
    }

    @Override
    public KeepAliveResponse build() {
      return new KeepAliveResponse(status, error, leader, members);
    }
  }
}
