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
package io.atomix.copycat.server.protocol.request;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.request.AbstractRequest;
import io.atomix.copycat.server.cluster.Member;

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
public class ConfigureRequest extends AbstractRequest {
  protected final long term;
  protected final int leader;
  protected final long index;
  protected final long timestamp;
  protected final Collection<Member> members;

  public ConfigureRequest(long term, int leader, long index, long timestamp, Collection<Member> members) {
    this.term = Assert.arg(term, term > 0, "term must be positive");
    this.leader = leader;
    this.index = Assert.argNot(index, index < 0, "index must be positive");
    this.timestamp = Assert.argNot(timestamp, timestamp <= 0, "timestamp must be positive");
    this.members = Assert.notNull(members, "members");
  }

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the requesting leader address.
   *
   * @return The leader's address.
   */
  public int leader() {
    return leader;
  }

  /**
   * Returns the configuration index.
   *
   * @return The configuration index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the configuration timestamp.
   *
   * @return The configuration timestamp.
   */
  public long timestamp() {
    return timestamp;
  }

  /**
   * Returns the configuration members.
   *
   * @return The configuration members.
   */
  public Collection<Member> members() {
    return members;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), term, leader, index, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ConfigureRequest) {
      ConfigureRequest request = (ConfigureRequest) object;
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
  public static class Builder extends AbstractRequest.Builder<ConfigureRequest.Builder, ConfigureRequest> {
    protected long term;
    protected int leader;
    protected long index;
    protected long timestamp;
    protected Collection<Member> members;

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code term} is not positive
     */
    public Builder withTerm(long term) {
      this.term = Assert.arg(term, term > 0, "term must be positive");
      return this;
    }

    /**
     * Sets the request leader.
     *
     * @param leader The request leader.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code leader} is not positive
     */
    public Builder withLeader(int leader) {
      this.leader = leader;
      return this;
    }

    /**
     * Sets the request index.
     *
     * @param index The request index.
     * @return The request builder.
     */
    public Builder withIndex(long index) {
      this.index = Assert.argNot(index, index < 0, "index must be positive");
      return this;
    }

    /**
     * Sets the request timestamp.
     *
     * @param timestamp The request timestamp.
     * @return The request builder.
     */
    public Builder withTime(long timestamp) {
      this.timestamp = Assert.argNot(timestamp, timestamp <= 0, "timestamp must be positive");
      return this;
    }

    /**
     * Sets the request members.
     *
     * @param members The request members.
     * @return The request builder.
     * @throws NullPointerException if {@code member} is null
     */
    public Builder withMembers(Collection<Member> members) {
      this.members = Assert.notNull(members, "members");
      return this;
    }

    @Override
    public ConfigureRequest build() {
      return new ConfigureRequest(term, leader, index, timestamp, members);
    }
  }
}
