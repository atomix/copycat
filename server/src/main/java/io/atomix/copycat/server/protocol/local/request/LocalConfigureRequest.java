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
package io.atomix.copycat.server.protocol.local.request;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.local.request.AbstractLocalRequest;
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
public class LocalConfigureRequest extends AbstractLocalRequest implements ConfigureRequest {
  private final long term;
  private final int leader;
  private final long index;
  private final long timestamp;
  private final Collection<Member> members;

  public LocalConfigureRequest(long term, int leader, long index, long timestamp, Collection<Member> members) {
    this.term = term;
    this.leader = leader;
    this.index = index;
    this.timestamp = timestamp;
    this.members = members;
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
    if (object instanceof LocalConfigureRequest) {
      LocalConfigureRequest request = (LocalConfigureRequest) object;
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
  public static class Builder extends AbstractLocalRequest.Builder<ConfigureRequest.Builder, ConfigureRequest> implements ConfigureRequest.Builder {
    private long term;
    private int leader;
    private long index;
    private long timestamp;
    private Collection<Member> members;

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
    public LocalConfigureRequest build() {
      return new LocalConfigureRequest(term, leader, index, timestamp, members);
    }
  }
}
