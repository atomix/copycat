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
package io.atomix.copycat.server.protocol.local.response;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.local.response.AbstractLocalResponse;
import io.atomix.copycat.server.protocol.response.AppendResponse;

import java.util.Objects;

/**
 * Server append entries response.
 * <p>
 * Append entries responses are sent by followers to leaders to indicate whether the handling of
 * an {@link io.atomix.copycat.server.protocol.request.AppendRequest} was successful. Failed append entries
 * requests do not result in {@link Status#ERROR} responses.
 * Instead, followers provide a successful response which indicates whether the append {@link #succeeded()}
 * and provides information regarding the follower's updated log to aid in resolving indexes on the leader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalAppendResponse extends AbstractLocalResponse implements AppendResponse {
  private final long term;
  private final boolean succeeded;
  private final long logIndex;

  public LocalAppendResponse(Status status, CopycatError error, long term, boolean succeeded, long logIndex) {
    super(status, error);
    this.term = term;
    this.succeeded = succeeded;
    this.logIndex = logIndex;
  }

  @Override
  public long term() {
    return term;
  }

  @Override
  public boolean succeeded() {
    return succeeded;
  }

  @Override
  public long logIndex() {
    return logIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, term, succeeded, logIndex);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LocalAppendResponse) {
      LocalAppendResponse response = (LocalAppendResponse) object;
      return response.status == status
        && response.term == term
        && response.succeeded == succeeded
        && response.logIndex == logIndex;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, term=%d, succeeded=%b, logIndex=%d]", getClass().getSimpleName(), status, term, succeeded, logIndex);
  }

  /**
   * Append response builder.
   */
  public static class Builder extends AbstractLocalResponse.Builder<AppendResponse.Builder, AppendResponse> implements AppendResponse.Builder {
    private long term;
    private boolean succeeded;
    private long logIndex;

    @Override
    public Builder withTerm(long term) {
      this.term = Assert.argNot(term, term <= 0, "term must be positive");
      return this;
    }

    @Override
    public Builder withSucceeded(boolean succeeded) {
      this.succeeded = succeeded;
      return this;
    }

    @Override
    public Builder withLogIndex(long index) {
      this.logIndex = Assert.argNot(index, index < 0, "term must not be negative");
      return this;
    }

    /**
     * @throws IllegalStateException if status is ok and term is not positive or log index is negative
     */
    @Override
    public LocalAppendResponse build() {
      return new LocalAppendResponse(status, error, term, succeeded, logIndex);
    }
  }
}
