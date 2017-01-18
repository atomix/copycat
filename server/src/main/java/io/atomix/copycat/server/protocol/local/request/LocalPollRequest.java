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
import io.atomix.copycat.server.protocol.request.PollRequest;

import java.util.Objects;

/**
 * Server poll request.
 * <p>
 * Poll requests aid in the implementation of the so-called "pre-vote" protocol. They are sent by followers
 * to all other servers prior to transitioning to the candidate state. This helps ensure that servers that
 * can't win elections do not disrupt existing leaders when e.g. rejoining the cluster after a partition.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalPollRequest extends AbstractLocalRequest implements PollRequest {
  private final long term;
  private final int candidate;
  private final long logIndex;
  private final long logTerm;

  public LocalPollRequest(long term, int candidate, long logIndex, long logTerm) {
    this.term = term;
    this.candidate = candidate;
    this.logIndex = logIndex;
    this.logTerm = logTerm;
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
    if (object instanceof LocalPollRequest) {
      LocalPollRequest request = (LocalPollRequest) object;
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
   * Poll request builder.
   */
  public static class Builder extends AbstractLocalRequest.Builder<PollRequest.Builder, PollRequest> implements PollRequest.Builder {
    private long term = -1;
    private int candidate;
    private long logIndex = -1;
    private long logTerm = -1;

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

    @Override
    public LocalPollRequest build() {
      return new LocalPollRequest(term, candidate, logIndex, logTerm);
    }
  }
}
