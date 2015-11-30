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
 * limitations under the License
 */
package io.atomix.copycat.server.request;

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.request.AbstractRequest;
import io.atomix.copycat.server.state.Member;

import java.util.Collection;
import java.util.Objects;

/**
 * Configuration installation request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=219)
public class ConfigureRequest extends AbstractRequest<ConfigureRequest> {

  /**
   * Returns a new configuration request builder.
   *
   * @return A new configuration request builder.
   */
  public static Builder builder() {
    return new Builder(new ConfigureRequest());
  }

  /**
   * Returns an configuration request builder for an existing request.
   *
   * @param request The request to build.
   * @return The configuration request builder.
   */
  public static Builder builder(ConfigureRequest request) {
    return new Builder(request);
  }

  private long term;
  private int leader;
  protected long version;
  protected Collection<Member> members;
  protected long snapshot;
  protected Buffer data;

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
   * Returns the configuration version.
   *
   * @return The configuration version.
   */
  public long version() {
    return version;
  }

  /**
   * Returns the configuration members.
   *
   * @return The configuration members.
   */
  public Collection<Member> members() {
    return members;
  }

  /**
   * Returns the snapshot index.
   *
   * @return The snapshot index.
   */
  public long snapshot() {
    return snapshot;
  }

  /**
   * Returns the snapshot data.
   *
   * @return The snapshot data.
   */
  public Buffer data() {
    return data;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(term).writeInt(leader).writeLong(version).writeLong(snapshot);
    serializer.writeObject(members, buffer);
    serializer.writeObject(data, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    term = buffer.readLong();
    leader = buffer.readInt();
    version = buffer.readLong();
    snapshot = buffer.readLong();
    members = serializer.readObject(buffer);
    data = serializer.readObject(buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), term, leader, version, snapshot, members);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ConfigureRequest) {
      ConfigureRequest request = (ConfigureRequest) object;
      return request.term == term
        && request.leader == leader
        && request.version == version
        && request.snapshot == snapshot
        && request.members.equals(members);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, leader=%d, version=%d, snapshot=%d, members=%s]", getClass().getSimpleName(), term, leader, version, snapshot, members);
  }

  /**
   * Heartbeat request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, ConfigureRequest> {
    protected Builder(ConfigureRequest request) {
      super(request);
    }

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code term} is not positive
     */
    public Builder withTerm(long term) {
      request.term = Assert.arg(term, term > 0, "term must be positive");
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
      request.leader = leader;
      return this;
    }

    /**
     * Sets the request version.
     *
     * @param version The request version.
     * @return The request builder.
     */
    public Builder withVersion(long version) {
      request.version = Assert.argNot(version, version < 0, "version must be positive");
      return this;
    }

    /**
     * Sets the request snapshot version.
     *
     * @param snapshot The snapshot version.
     * @return The request builder.
     */
    public Builder withSnapshot(long snapshot) {
      request.snapshot = Assert.argNot(snapshot, snapshot < 0, "snapshot must be positive");
      return this;
    }

    /**
     * Sets the request snapshot data.
     *
     * @param data The request snapshot data.
     * @return The request builder.
     * @throws NullPointerException if {@code member} is null
     */
    public Builder withData(Buffer data) {
      request.data = Assert.notNull(data, "data");
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
      request.members = Assert.notNull(members, "members");
      return this;
    }

    /**
     * @throws IllegalStateException if member is null
     */
    @Override
    public ConfigureRequest build() {
      super.build();
      Assert.stateNot(request.term <= 0, "term must be positive");
      Assert.argNot(request.version < 0, "version must be positive");
      Assert.notNull(request.members, "members");
      return request;
    }
  }

}
