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

import io.atomix.copycat.util.Assert;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.response.AbstractResponse;

import java.util.Objects;

/**
 * Server vote response.
 * <p>
 * Vote responses are sent by active servers in response to vote requests by candidate to indicate
 * whether the responding server voted for the requesting candidate. This is indicated by the
 * {@link #voted()} field of the response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VoteResponse extends AbstractResponse {
  protected final long term;
  protected final boolean voted;

  public VoteResponse(Status status, CopycatError error, long term, boolean voted) {
    super(status, error);
    this.term = Assert.argNot(term, term < 0, "term must be positive");
    this.voted = voted;
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
   * Returns a boolean indicating whether the vote was granted.
   *
   * @return Indicates whether the vote was granted.
   */
  public boolean voted() {
    return voted;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, term, voted);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof VoteResponse) {
      VoteResponse response = (VoteResponse) object;
      return response.status == status
        && response.term == term
        && response.voted == voted;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, term=%d, voted=%b]", getClass().getSimpleName(), status, term, voted);
  }

  /**
   * Poll response builder.
   */
  public static class Builder extends AbstractResponse.Builder<VoteResponse.Builder, VoteResponse> {
    protected long term;
    protected boolean voted;

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The vote response builder.
     * @throws IllegalArgumentException if {@code term} is negative
     */
    public Builder withTerm(long term) {
      this.term = Assert.argNot(term, term < 0, "term cannot be negative");
      return this;
    }

    /**
     * Sets whether the vote was granted.
     *
     * @param voted Whether the vote was granted.
     * @return The vote response builder.
     */
    public Builder withVoted(boolean voted) {
      this.voted = voted;
      return this;
    }

    @Override
    public VoteResponse copy(VoteResponse response) {
      return new VoteResponse(response.status, response.error, response.term, response.voted);
    }

    @Override
    public VoteResponse build() {
      return new VoteResponse(status, error, term, voted);
    }
  }
}
