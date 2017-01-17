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
package io.atomix.copycat.server.protocol.net.request;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.net.request.AbstractNetRequest;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.request.ConfigureRequest;

import java.util.Collection;
import java.util.Objects;

/**
 * Configuration installation request.
 * <p>
 * Configuration requests are special requests that aid in installing committed configurations
 * to passive and reserve members of the cluster. Prior to the start of replication from an active
 * member to a passive or reserve member, the active member must update the passive/reserve member's
 * configuration to ensure it is in the expected state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetConfigureRequest extends AbstractNetRequest implements ConfigureRequest, RaftNetRequest {
  private final long term;
  private final int leader;
  private final long index;
  private final long timestamp;
  private final Collection<Member> members;

  public NetConfigureRequest(long id, long term, int leader, long index, long timestamp, Collection<Member> members) {
    super(id);
    this.term = term;
    this.leader = leader;
    this.index = index;
    this.timestamp = timestamp;
    this.members = members;
  }

  @Override
  public Type type() {
    return RaftNetRequest.Types.CONFIGURE_REQUEST;
  }

  @Override
  public long term() {
    return term;
  }

  @Override
  public int leader() {
    return leader;
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public long timestamp() {
    return timestamp;
  }

  @Override
  public Collection<Member> members() {
    return members;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), term, leader, index, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NetConfigureRequest) {
      NetConfigureRequest request = (NetConfigureRequest) object;
      return request.term == term
        && request.leader == leader
        && request.index == index
        && request.timestamp == timestamp
        && request.members.equals(members);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, leader=%d, index=%d, timestamp=%d, members=%s]", getClass().getSimpleName(), term, leader, index, timestamp, members);
  }

  /**
   * Heartbeat request builder.
   */
  public static class Builder extends AbstractNetRequest.Builder<ConfigureRequest.Builder, ConfigureRequest> implements ConfigureRequest.Builder {
    private long term;
    private int leader;
    private long index;
    private long timestamp;
    private Collection<Member> members;

    public Builder(long id) {
      super(id);
    }

    @Override
    public Builder withTerm(long term) {
      this.term = Assert.arg(term, term > 0, "term must be positive");
      return this;
    }

    @Override
    public Builder withLeader(int leader) {
      this.leader = leader;
      return this;
    }

    @Override
    public Builder withIndex(long index) {
      this.index = Assert.argNot(index, index < 0, "index must be positive");
      return this;
    }

    @Override
    public Builder withTime(long timestamp) {
      this.timestamp = Assert.argNot(timestamp, timestamp <= 0, "timestamp must be positive");
      return this;
    }

    @Override
    public Builder withMembers(Collection<Member> members) {
      this.members = Assert.notNull(members, "members");
      return this;
    }

    @Override
    public NetConfigureRequest build() {
      return new NetConfigureRequest(id, term, leader, index, timestamp, members);
    }
  }

  /**
   * Configure request serializer.
   */
  public static class Serializer extends AbstractNetRequest.Serializer<NetConfigureRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetConfigureRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.term);
      output.writeInt(request.leader);
      output.writeLong(request.index);
      output.writeLong(request.timestamp);
      kryo.writeObject(output, request.members);
    }

    @Override
    @SuppressWarnings("unchecked")
    public NetConfigureRequest read(Kryo kryo, Input input, Class<NetConfigureRequest> type) {
      return new NetConfigureRequest(input.readLong(), input.readLong(), input.readInt(), input.readLong(), input.readLong(), kryo.readObject(input, Collection.class));
    }
  }
}
