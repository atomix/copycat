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
package io.atomix.copycat.server.protocol;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.AbstractRequest;

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
  protected long index;
  protected int offset;
  protected byte[] data;
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
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(term)
      .writeInt(leader)
      .writeLong(index)
      .writeInt(offset)
      .writeBoolean(complete);
    serializer.writeObject(data, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    term = buffer.readLong();
    leader = buffer.readInt();
    index = buffer.readLong();
    offset = buffer.readInt();
    complete = buffer.readBoolean();
    data = serializer.<byte[]>readObject(buffer);
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
     * Sets the request index.
     *
     * @param index The request index.
     * @return The request builder.
     */
    public Builder withIndex(long index) {
      request.index = Assert.argNot(index, index < 0, "index must be positive");
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
     * Sets the request snapshot bytes.
     *
     * @param snapshot The snapshot bytes.
     * @return The request builder.
     */
    public Builder withData(byte[] snapshot) {
      request.data = Assert.notNull(snapshot, "data");
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
      Assert.argNot(request.index < 0, "index must be positive");
      Assert.notNull(request.data, "data");
      return request;
    }
  }

}
