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

import java.util.Objects;

/**
 * Snapshot installation request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=229)
public class InstallRequest extends AbstractRequest<InstallRequest> {

  /**
   * Returns a new install request builder.
   *
   * @return A new install request builder.
   */
  public static Builder builder() {
    return new Builder(new InstallRequest());
  }

  /**
   * Returns an install request builder for an existing request.
   *
   * @param request The request to build.
   * @return The install request builder.
   */
  public static Builder builder(InstallRequest request) {
    return new Builder(request);
  }

  private long term;
  private int leader;
  protected long version;
  protected int offset;
  protected Buffer snapshot;
  protected boolean complete;

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
   * Returns the snapshot version.
   *
   * @return The snapshot version.
   */
  public long version() {
    return version;
  }

  /**
   * Returns the offset of the snapshot chunk.
   *
   * @return The offset of the snapshot chunk.
   */
  public int offset() {
    return offset;
  }

  /**
   * Returns the snapshot index.
   *
   * @return The snapshot index.
   */
  public Buffer snapshot() {
    return snapshot;
  }

  /**
   * Returns a boolean value indicating whether this is the last chunk of the snapshot.
   *
   * @return Indicates whether this request is the last chunk of the snapshot.
   */
  public boolean complete() {
    return complete;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(term)
      .writeInt(leader)
      .writeLong(version)
      .writeInt(offset)
      .writeBoolean(complete);
    serializer.writeObject(snapshot, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    term = buffer.readLong();
    leader = buffer.readInt();
    version = buffer.readLong();
    offset = buffer.readInt();
    complete = buffer.readBoolean();
    snapshot = serializer.readObject(buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), term, leader, version, offset, complete, snapshot);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof InstallRequest) {
      InstallRequest request = (InstallRequest) object;
      return request.term == term
        && request.leader == leader
        && request.version == version
        && request.offset == offset
        && request.complete == complete
        && request.snapshot == snapshot;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, leader=%d, version=%d, offset=%d, snapshot=%s, complete=%b]", getClass().getSimpleName(), term, leader, version, offset, snapshot, complete);
  }

  /**
   * Heartbeat request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, InstallRequest> {
    protected Builder(InstallRequest request) {
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
     * Sets the request offset.
     *
     * @param offset The request offset.
     * @return The request builder.
     */
    public Builder withOffset(int offset) {
      request.offset = Assert.argNot(offset, offset < 0, "offset must be positive");
      return this;
    }

    /**
     * Sets the request snapshot buffer.
     *
     * @param snapshot The snapshot buffer.
     * @return The request builder.
     */
    public Builder withSnapshot(Buffer snapshot) {
      request.snapshot = Assert.notNull(snapshot, "snapshot");
      return this;
    }

    /**
     * Sets whether the request is complete.
     *
     * @param complete Whether the snapshot is complete.
     * @return The request builder.
     * @throws NullPointerException if {@code member} is null
     */
    public Builder withComplete(boolean complete) {
      request.complete = complete;
      return this;
    }

    /**
     * @throws IllegalStateException if member is null
     */
    @Override
    public InstallRequest build() {
      super.build();
      Assert.stateNot(request.term <= 0, "term must be positive");
      Assert.argNot(request.version < 0, "version must be positive");
      Assert.notNull(request.snapshot, "snapshot");
      return request;
    }
  }

}
