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
 * Session register response.
 * <p>
 * Session register responses are sent in response to {@link RegisterRequest}s
 * sent by a client. Upon the successful registration of a session, the register response will contain the
 * registered {@link #session()} identifier, the session {@link #timeout()}, and the current cluster
 * {@link #leader()} and {@link #members()} to allow the client to make intelligent decisions about
 * connecting to and communicating with the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RegisterResponse extends AbstractResponse {

  /**
   * Returns a new register client response builder.
   *
   * @return A new register client response builder.
   */
  public static Builder builder() {
    return new Builder(new RegisterResponse());
  }

  /**
   * Returns a register client response builder for an existing response.
   *
   * @param response The response to build.
   * @return The register client response builder.
   * @throws NullPointerException if {@code response} is null
   */
  public static Builder builder(RegisterResponse response) {
    return new Builder(response);
  }

  private long session;
  private Address leader;
  private Collection<Address> members;
  private long timeout;

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
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      session = buffer.readLong();
      timeout = buffer.readLong();
      leader = serializer.readObject(buffer);
      members = serializer.readObject(buffer);
    } else {
      error = CopycatError.forId(buffer.readByte());
      session = 0;
      members = null;
    }
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      buffer.writeLong(session);
      buffer.writeLong(timeout);
      serializer.writeObject(leader, buffer);
      serializer.writeObject(members, buffer);
    } else {
      buffer.writeByte(error.id());
    }
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
    return String.format("%s[status=%s, error=%s, session=%d, leader=%s, members=%s]", getClass().getSimpleName(), status, error, session, leader, members);
  }

  /**
   * Register response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, RegisterResponse> {
    protected Builder(RegisterResponse response) {
      super(response);
    }

    /**
     * Sets the response session ID.
     *
     * @param session The session ID.
     * @return The register response builder.
     * @throws IllegalArgumentException if {@code session} is less than 1
     */
    public Builder withSession(long session) {
      response.session = Assert.argNot(session, session < 1, "session must be positive");
      return this;
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
     * Sets the session timeout.
     *
     * @param timeout The session timeout.
     * @return The register response builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     */
    public Builder withTimeout(long timeout) {
      response.timeout = Assert.argNot(timeout, timeout <= 0, "timeout must be positive");
      return this;
    }

    /**
     * @throws IllegalStateException if status is OK and members is null
     */
    @Override
    public RegisterResponse build() {
      super.build();
      Assert.stateNot(response.status == Status.OK && response.members == null, "members cannot be null");
      Assert.stateNot(response.status == Status.OK && response.timeout <= 0, "timeout must be positive");
      return response;
    }
  }

}
