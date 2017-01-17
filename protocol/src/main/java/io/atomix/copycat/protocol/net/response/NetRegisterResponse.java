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
public class NetRegisterResponse extends AbstractNetResponse implements RegisterResponse {
  private final long session;
  private final Address leader;
  private final Collection<Address> members;
  private final long timeout;

  protected NetRegisterResponse(long id, Status status, CopycatError error, long session, Address leader, Collection<Address> members, long timeout) {
    super(id, status, error);
    this.session = session;
    this.leader = leader;
    this.members = members;
    this.timeout = timeout;
  }

  @Override
  public Type type() {
    return Types.REGISTER_RESPONSE;
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
    if (object instanceof NetRegisterResponse) {
      NetRegisterResponse response = (NetRegisterResponse) object;
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
  public static class Builder extends AbstractNetResponse.Builder<RegisterResponse.Builder, RegisterResponse> implements RegisterResponse.Builder {
    private long session;
    private Address leader;
    private Collection<Address> members;
    private long timeout;

    public Builder(long id) {
      super(id);
    }

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
      return new NetRegisterResponse(id, status, error, session, leader, members, timeout);
    }
  }

  /**
   * Register response serializer.
   */
  public static class Serializer extends AbstractNetResponse.Serializer<NetRegisterResponse> {
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
