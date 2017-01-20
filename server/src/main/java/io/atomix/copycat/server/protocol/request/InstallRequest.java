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

import io.atomix.copycat.util.Assert;
import io.atomix.copycat.protocol.request.AbstractRequest;

import java.util.Arrays;
import java.util.Objects;

/**
 * Server snapshot installation request.
 * <p>
 * Snapshot installation requests are sent by the leader to a follower when the follower indicates
 * that its log is further behind than the last snapshot taken by the leader. Snapshots are sent
 * in chunks, with each chunk being sent in a separate install request. As requests are received by
 * the follower, the snapshot is reconstructed based on the provided {@link #offset()} and other
 * metadata. The last install request will be sent with {@link #complete()} being {@code true} to
 * indicate that all chunks of the snapshot have been sent.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class InstallRequest extends AbstractRequest {
  protected final long term;
  protected final int leader;
  protected final long index;
  protected final int offset;
  protected final byte[] data;
  protected final boolean complete;

  public InstallRequest(long term, int leader, long index, int offset, byte[] data, boolean complete) {
    this.term = Assert.arg(term, term > 0, "term must be positive");
    this.leader = leader;
    this.index = Assert.argNot(index, index < 0, "index must be positive");
    this.offset = Assert.argNot(offset, offset < 0, "offset must be positive");
    this.data = Assert.notNull(data, "data");
    this.complete = complete;
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
   * Returns the snapshot index.
   *
   * @return The snapshot index.
   */
  public long index() {
    return index;
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
   * Returns the snapshot data.
   *
   * @return The snapshot data.
   */
  public byte[] data() {
    return data;
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
  public int hashCode() {
    return Objects.hash(getClass(), term, leader, index, offset, complete, data);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof InstallRequest) {
      InstallRequest request = (InstallRequest) object;
      return request.term == term
        && request.leader == leader
        && request.index == index
        && request.offset == offset
        && request.complete == complete
        && Arrays.equals(request.data, data);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, leader=%d, index=%d, offset=%d, data=%s, complete=%b]", getClass().getSimpleName(), term, leader, index, offset, data, complete);
  }

  /**
   * Snapshot request builder.
   */
  public static class Builder extends AbstractRequest.Builder<InstallRequest.Builder, InstallRequest> {
    protected long term;
    protected int leader;
    protected long index;
    protected int offset;
    protected byte[] data;
    protected boolean complete;

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
     * Sets the request offset.
     *
     * @param offset The request offset.
     * @return The request builder.
     */
    public Builder withOffset(int offset) {
      this.offset = Assert.argNot(offset, offset < 0, "offset must be positive");
      return this;
    }

    /**
     * Sets the request snapshot bytes.
     *
     * @param snapshot The snapshot bytes.
     * @return The request builder.
     */
    public Builder withData(byte[] snapshot) {
      this.data = Assert.notNull(snapshot, "data");
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
      this.complete = complete;
      return this;
    }

    @Override
    public InstallRequest copy(InstallRequest request) {
      return new InstallRequest(request.term, request.leader, request.index, request.offset, request.data, request.complete);
    }

    @Override
    public InstallRequest build() {
      return new InstallRequest(term, leader, index, offset, data, complete);
    }
  }
}
