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

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.net.request.AbstractNetRequest;
import io.atomix.copycat.server.protocol.request.VoteRequest;

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
public class NetVoteRequest extends AbstractNetRequest implements VoteRequest, RaftNetRequest {
  private final long term;
  private final int candidate;
  private final long logIndex;
  private final long logTerm;

  public NetVoteRequest(long id, long term, int candidate, long logIndex, long logTerm) {
    super(id);
    this.term = term;
    this.candidate = candidate;
    this.logIndex = logIndex;
    this.logTerm = logTerm;
  }

  @Override
  public Type type() {
    return RaftNetRequest.Types.VOTE_REQUEST;
  }

  @Override
  public long term() {
    return term;
  }

  @Override
  public int candidate() {
    return candidate;
  }

  @Override
  public long logIndex() {
    return logIndex;
  }

  @Override
  public long logTerm() {
    return logTerm;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), term, candidate, logIndex, logTerm);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NetVoteRequest) {
      NetVoteRequest request = (NetVoteRequest) object;
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
  public static class Builder extends AbstractNetRequest.Builder<VoteRequest.Builder, VoteRequest> implements VoteRequest.Builder {
    private long term = -1;
    private int candidate;
    private long logIndex = -1;
    private long logTerm = -1;

    public Builder(long id) {
      super(id);
    }

    @Override
    public Builder withTerm(long term) {
      this.term = Assert.argNot(term, term < 0, "term must not be negative");
      return this;
    }

    @Override
    public Builder withCandidate(int candidate) {
      this.candidate = candidate;
      return this;
    }

    @Override
    public Builder withLogIndex(long index) {
      this.logIndex = Assert.argNot(index, index < 0, "log index must not be negative");
      return this;
    }

    @Override
    public Builder withLogTerm(long term) {
      this.logTerm = Assert.argNot(term, term < 0,"log term must not be negative");
      return this;
    }

    /**
     * @throws IllegalStateException if candidate is not positive or if term, logIndex or logTerm are negative
     */
    @Override
    public NetVoteRequest build() {
      return new NetVoteRequest(id, term, candidate, logIndex, logTerm);
    }
  }

}
