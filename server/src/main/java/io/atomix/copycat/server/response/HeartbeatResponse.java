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
package io.atomix.copycat.server.response;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.error.RaftError;
import io.atomix.copycat.client.response.AbstractResponse;

import java.util.Objects;

/**
 * Server heartbeat response.
 * <p>
 * Heartbeat responses are sent by the leader upon the success of a heartbeat. Success indicates that
 * a {@link io.atomix.copycat.server.storage.entry.HeartbeatEntry} was committed to the Raft log. The
 * response contains information regarding the current cluster, including the {@link #term()} and {@link #leader()}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=218)
public class HeartbeatResponse extends AbstractResponse<HeartbeatResponse> {

  /**
   * Returns a new heartbeat response builder.
   *
   * @return A new heartbeat response builder.
   */
  public static Builder builder() {
    return new Builder(new HeartbeatResponse());
  }

  /**
   * Returns a heartbeat response builder for an existing response.
   *
   * @param response The response to build.
   * @return The heartbeat response builder.
   */
  public static Builder builder(HeartbeatResponse response) {
    return new Builder(response);
  }

  private long term;
  private int leader;

  /**
   * Returns the responding node's current term.
   *
   * @return The responding node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the responding node's current leader.
   *
   * @return The responding node's current leader.
   */
  public int leader() {
    return leader;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      term = buffer.readLong();
      leader = buffer.readInt();
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      buffer.writeLong(term)
        .writeInt(leader);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, term, leader);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof HeartbeatResponse) {
      HeartbeatResponse response = (HeartbeatResponse) object;
      return response.status == status
        && response.term == term
        && response.leader == leader;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, term=%d, leader=%d]", getClass().getSimpleName(), status, term, leader);
  }

  /**
   * Heartbeat response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, HeartbeatResponse> {
    protected Builder(HeartbeatResponse response) {
      super(response);
    }

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The append response builder
     * @throws IllegalArgumentException if {@code term} is not positive
     */
    public Builder withTerm(long term) {
      response.term = Assert.argNot(term, term <= 0, "term must be positive");
      return this;
    }

    /**
     * Sets the response leader.
     *
     * @param leader The response leader.
     * @return The append response builder.
     */
    public Builder withLeader(int leader) {
      response.leader = leader;
      return this;
    }

    /**
     * @throws IllegalStateException if status is ok and term is not positive or log index is negative
     */
    @Override
    public HeartbeatResponse build() {
      super.build();
      if (response.status == Status.OK) {
        Assert.stateNot(response.term < 0, "term must be positive");
      }
      return response;
    }
  }

}
