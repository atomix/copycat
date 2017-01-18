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
package io.atomix.copycat.protocol.local.response;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.response.RegisterResponse;
import io.atomix.copycat.protocol.websocket.request.WebSocketRegisterRequest;

import java.util.Collection;
import java.util.Objects;

/**
 * Session register response.
 * <p>
 * Session register responses are sent in response to {@link WebSocketRegisterRequest}s
 * sent by a client. Upon the successful registration of a session, the register response will contain the
 * registered {@link #session()} identifier, the session {@link #timeout()}, and the current cluster
 * {@link #leader()} and {@link #members()} to allow the client to make intelligent decisions about
 * connecting to and communicating with the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalRegisterResponse extends AbstractLocalResponse implements RegisterResponse {
  private final long session;
  private final Address leader;
  private final Collection<Address> members;
  private final long timeout;

  protected LocalRegisterResponse(Status status, CopycatError error, long session, Address leader, Collection<Address> members, long timeout) {
    super(status, error);
    this.session = session;
    this.leader = leader;
    this.members = members;
    this.timeout = timeout;
  }

  @Override
  public long session() {
    return session;
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
  public long timeout() {
    return timeout;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, session, leader, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LocalRegisterResponse) {
      LocalRegisterResponse response = (LocalRegisterResponse) object;
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
  public static class Builder extends AbstractLocalResponse.Builder<RegisterResponse.Builder, RegisterResponse> implements RegisterResponse.Builder {
    private long session;
    private Address leader;
    private Collection<Address> members;
    private long timeout;

    @Override
    public Builder withSession(long session) {
      this.session = Assert.argNot(session, session < 1, "session must be positive");
      return this;
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
    public Builder withTimeout(long timeout) {
      this.timeout = Assert.argNot(timeout, timeout <= 0, "timeout must be positive");
      return this;
    }

    @Override
    public RegisterResponse build() {
      return new LocalRegisterResponse(status, error, session, leader, members, timeout);
    }
  }
}
