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
import io.atomix.copycat.server.protocol.response.PollResponse;

import java.util.Objects;

/**
 * Server poll response.
 * <p>
 * Poll responses are sent by active servers in response to poll requests by followers to indicate
 * whether the responding server would vote for the requesting server if it were a candidate. This is
 * indicated by the {@link #accepted()} field of the response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalPollResponse extends AbstractLocalResponse implements PollResponse {
  private final long term;
  private final boolean accepted;

  public LocalPollResponse(Status status, CopycatError error, long term, boolean accepted) {
    super(status, error);
    this.term = term;
    this.accepted = accepted;
  }

  /**
   * Returns the responding node's current term.
   *
   * @return The responding node's current term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns a boolean indicating whether the poll was accepted.
   *
   * @return Indicates whether the poll was accepted.
   */
  public boolean accepted() {
    return accepted;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, term, accepted);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LocalPollResponse) {
      LocalPollResponse response = (LocalPollResponse) object;
      return response.status == status
        && response.term == term
        && response.accepted == accepted;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, term=%d, accepted=%b]", getClass().getSimpleName(), status, term, accepted);
  }

  /**
   * Poll response builder.
   */
  public static class Builder extends AbstractLocalResponse.Builder<PollResponse.Builder, PollResponse> implements PollResponse.Builder {
    private long term;
    private boolean accepted;

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The poll response builder.
     * @throws IllegalArgumentException if {@code term} is not positive
     */
    public Builder withTerm(long term) {
      this.term = Assert.argNot(term, term < 0, "term must be positive");
      return this;
    }

    /**
     * Sets whether the poll was granted.
     *
     * @param accepted Whether the poll was granted.
     * @return The poll response builder.
     */
    public Builder withAccepted(boolean accepted) {
      this.accepted = accepted;
      return this;
    }

    /**
     * @throws IllegalStateException if status is OK and {@code term} is not positive
     */
    @Override
    public LocalPollResponse build() {
      return new LocalPollResponse(status, error, term, accepted);
    }
  }
}
