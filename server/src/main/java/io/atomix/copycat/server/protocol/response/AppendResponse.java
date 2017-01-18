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
package io.atomix.copycat.server.protocol.response;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.response.AbstractResponse;

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
public class AppendResponse extends AbstractResponse {
  protected final long term;
  protected final boolean succeeded;
  protected final long logIndex;

  public AppendResponse(Status status, CopycatError error, long term, boolean succeeded, long logIndex) {
    super(status, error);
    this.term = Assert.argNot(term, term <= 0, "term must be positive");
    this.succeeded = succeeded;
    this.logIndex = Assert.argNot(logIndex, logIndex < 0, "logIndex must not be negative");
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
   * Returns a boolean indicating whether the append was successful.
   *
   * @return Indicates whether the append was successful.
   */
  public boolean succeeded() {
    return succeeded;
  }

  /**
   * Returns the last index of the replica's log.
   *
   * @return The last index of the responding replica's log.
   */
  public long logIndex() {
    return logIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, term, succeeded, logIndex);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof AppendResponse) {
      AppendResponse response = (AppendResponse) object;
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
  public static class Builder extends AbstractResponse.Builder<AppendResponse.Builder, AppendResponse> {
    protected long term;
    protected boolean succeeded;
    protected long logIndex;

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The append response builder
     * @throws IllegalArgumentException if {@code term} is not positive
     */
    public Builder withTerm(long term) {
      this.term = Assert.argNot(term, term <= 0, "term must be positive");
      return this;
    }

    /**
     * Sets whether the request succeeded.
     *
     * @param succeeded Whether the append request succeeded.
     * @return The append response builder.
     */
    public Builder withSucceeded(boolean succeeded) {
      this.succeeded = succeeded;
      return this;
    }

    /**
     * Sets the last index of the replica's log.
     *
     * @param index The last index of the replica's log.
     * @return The append response builder.
     * @throws IllegalArgumentException if {@code index} is negative
     */
    public Builder withLogIndex(long index) {
      this.logIndex = Assert.argNot(index, index < 0, "logIndex must not be negative");
      return this;
    }

    @Override
    public AppendResponse copy(AppendResponse response) {
      return new AppendResponse(response.status, response.error, response.term, response.succeeded, response.logIndex);
    }

    @Override
    public AppendResponse build() {
      return new AppendResponse(status, error, term, succeeded, logIndex);
    }
  }
}
