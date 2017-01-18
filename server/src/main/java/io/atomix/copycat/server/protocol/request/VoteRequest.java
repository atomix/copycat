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
  protected final long term;
  protected final int candidate;
  protected final long logIndex;
  protected final long logTerm;

  public VoteRequest(long term, int candidate, long logIndex, long logTerm) {
    this.term = Assert.argNot(term, term < 0, "term must not be negative");
    this.candidate = candidate;
    this.logIndex = Assert.argNot(logIndex, logIndex < 0, "logIndex must not be negative");
    this.logTerm = Assert.argNot(logTerm, logTerm < 0,"logTerm must not be negative");
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
   * Returns the candidate's ID.
   *
   * @return The candidate's ID.
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
  public static class Builder extends AbstractRequest.Builder<VoteRequest.Builder, VoteRequest> {
    protected long term = -1;
    protected int candidate;
    protected long logIndex = -1;
    protected long logTerm = -1;

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The poll request builder.
     * @throws IllegalArgumentException if {@code term} is negative
     */
    public Builder withTerm(long term) {
      this.term = Assert.argNot(term, term < 0, "term must not be negative");
      return this;
    }

    /**
     * Sets the request candidate.
     *
     * @param candidate The request candidate.
     * @return The poll request builder.
     * @throws IllegalArgumentException if {@code candidate} is not positive
     */
    public Builder withCandidate(int candidate) {
      this.candidate = candidate;
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
      this.logIndex = Assert.argNot(index, index < 0, "log index must not be negative");
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
      this.logTerm = Assert.argNot(term, term < 0,"log term must not be negative");
      return this;
    }

    @Override
    public VoteRequest copy(VoteRequest request) {
      return new VoteRequest(request.term, request.candidate, request.logIndex, request.logTerm);
    }

    @Override
    public VoteRequest build() {
      return new VoteRequest(term, candidate, logIndex, logTerm);
    }
  }
}
