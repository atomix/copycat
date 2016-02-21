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
 * limitations under the License.
 */
package io.atomix.copycat.server.protocol;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.AbstractRequest;

import java.util.Objects;

/**
 * Server vote request.
 * <p>
 * Vote requests are sent by candidate servers during an election to determine whether they should
 * become the leader for a cluster. Vote requests contain the necessary information for followers to
 * determine whether a candidate should receive their vote based on log and other information.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VoteRequest extends AbstractRequest {

  /**
   * Returns a new vote request builder.
   *
   * @return A new vote request builder.
   */
  public static Builder builder() {
    return new Builder(new VoteRequest());
  }

  /**
   * Returns a vote request builder for an existing request.
   *
   * @param request The request to build.
   * @return The vote request builder.
   */
  public static Builder builder(VoteRequest request) {
    return new Builder(request);
  }

  private long term = -1;
  private int candidate;
  private long logIndex = -1;
  private long logTerm = -1;

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the candidate's address.
   *
   * @return The candidate's address.
   */
  public int candidate() {
    return candidate;
  }

  /**
   * Returns the candidate's last log index.
   *
   * @return The candidate's last log index.
   */
  public long logIndex() {
    return logIndex;
  }

  /**
   * Returns the candidate's last log term.
   *
   * @return The candidate's last log term.
   */
  public long logTerm() {
    return logTerm;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    term = buffer.readLong();
    candidate = buffer.readInt();
    logIndex = buffer.readLong();
    logTerm = buffer.readLong();
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(term)
      .writeInt(candidate)
      .writeLong(logIndex)
      .writeLong(logTerm);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), term, candidate, logIndex, logTerm);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof VoteRequest) {
      VoteRequest request = (VoteRequest) object;
      return request.term == term
        && request.candidate == candidate
        && request.logIndex == logIndex
        && request.logTerm == logTerm;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[term=%d, candidate=%s, logIndex=%d, logTerm=%d]", getClass().getSimpleName(), term, candidate, logIndex, logTerm);
  }

  /**
   * Vote request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, VoteRequest> {
    protected Builder(VoteRequest request) {
      super(request);
    }

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The poll request builder.
     * @throws IllegalArgumentException if {@code term} is negative
     */
    public Builder withTerm(long term) {
      request.term = Assert.argNot(term, term < 0, "term must not be negative");
      return this;
    }

    /**
     * Sets the request leader.
     *
     * @param candidate The request candidate.
     * @return The poll request builder.
     * @throws IllegalArgumentException if {@code candidate} is not positive
     */
    public Builder withCandidate(int candidate) {
      request.candidate = candidate;
      return this;
    }

    /**
     * Sets the request last log index.
     *
     * @param index The request last log index.
     * @return The poll request builder.
     * @throws IllegalArgumentException if {@code index} is negative
     */
    public Builder withLogIndex(long index) {
      request.logIndex = Assert.argNot(index, index < 0, "log index must not be negative");
      return this;
    }

    /**
     * Sets the request last log term.
     *
     * @param term The request last log term.
     * @return The poll request builder.
     * @throws IllegalArgumentException if {@code term} is negative
     */
    public Builder withLogTerm(long term) {
      request.logTerm = Assert.argNot(term, term < 0,"log term must not be negative");
      return this;
    }

    /**
     * @throws IllegalStateException if candidate is not positive or if term, logIndex or logTerm are negative
     */
    @Override
    public VoteRequest build() {
      super.build();
      Assert.stateNot(request.term < 0, "term must not be negative");
      Assert.stateNot(request.logIndex < 0, "log index must not be negative");
      Assert.stateNot(request.logTerm < 0, "log term must not be negative");
      return request;
    }

    @Override
    public int hashCode() {
      return Objects.hash(request);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).request.equals(request);
    }

    @Override
    public String toString() {
      return String.format("%s[request=%s]", getClass().getCanonicalName(), request);
    }

  }

}
