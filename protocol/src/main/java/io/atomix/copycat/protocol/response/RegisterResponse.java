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

import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.util.Assert;

import java.util.Collection;
import java.util.Objects;

/**
 * Session register response.
 * <p>
 * Session register responses are sent in response to {@link io.atomix.copycat.protocol.request.RegisterRequest}
 * sent by a client. Upon the successful registration of a session, the register response will contain the
 * registered {@link #session()} identifier, the session {@link #timeout()}, and the current cluster
 * {@link #leader()} and {@link #members()} to allow the client to make intelligent decisions about
 * connecting to and communicating with the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RegisterResponse extends AbstractResponse {

  /**
   * Returns a new register response builder.
   *
   * @return A new register response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  protected final long session;
  protected final Address leader;
  protected final Collection<Address> members;
  protected final long timeout;

  public RegisterResponse(Status status, ProtocolResponse.Error error, long session, Address leader, Collection<Address> members, long timeout) {
    super(status, error);
    if (status == Status.OK) {
      this.session = Assert.argNot(session, session < 1, "session must be positive");
      this.leader = leader;
      this.members = Assert.notNull(members, "members");
      this.timeout = Assert.argNot(timeout, timeout <= 0, "timeout must be positive");
    } else {
      this.session = 0;
      this.leader = null;
      this.members = null;
      this.timeout = 0;
    }
  }

  @Override
  public Type type() {
    return Type.REGISTER;
  }

  /**
   * Returns the registered session ID.
   *
   * @return The registered session ID.
   */
  public long session() {
    return session;
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

  /**
   * Returns the client session timeout.
   *
   * @return The client session timeout.
   */
  public long timeout() {
    return timeout;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, session, leader, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof RegisterResponse) {
      RegisterResponse response = (RegisterResponse) object;
      return response.status == status
        && response.session == session
        && ((response.leader == null && leader == null)
        || (response.leader != null && leader != null && response.leader.equals(leader)))
        && ((response.members == null && members == null)
        || (response.members != null && members != null && response.members.equals(members)))
        && response.timeout == timeout;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, session=%d, leader=%s, members=%s]", getClass().getSimpleName(), status, session, leader, members);
  }

  /**
   * Register response builder.
   */
  public static class Builder extends AbstractResponse.Builder<RegisterResponse.Builder, RegisterResponse> {
    protected long session;
    protected Address leader;
    protected Collection<Address> members;
    protected long timeout;

    /**
     * Sets the response session ID.
     *
     * @param session The session ID.
     * @return The register response builder.
     * @throws IllegalArgumentException if {@code session} is less than 1
     */
    public Builder withSession(long session) {
      this.session = session;
      return this;
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
      this.members = members;
      return this;
    }

    /**
     * Sets the session timeout.
     *
     * @param timeout The session timeout.
     * @return The register response builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     */
    public Builder withTimeout(long timeout) {
      this.timeout = timeout;
      return this;
    }

    @Override
    public RegisterResponse copy(RegisterResponse response) {
      return new RegisterResponse(response.status, response.error, response.session, response.leader, response.members, response.timeout);
    }

    @Override
    public RegisterResponse build() {
      return new RegisterResponse(status, error, session, leader, members, timeout);
    }
  }
}
