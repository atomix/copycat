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
import io.atomix.copycat.server.protocol.response.VoteResponse;

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
public class LocalVoteResponse extends AbstractLocalResponse implements VoteResponse {
  private final long term;
  private final boolean voted;

  public LocalVoteResponse(Status status, CopycatError error, long term, boolean voted) {
    super(status, error);
    this.term = term;
    this.voted = voted;
  }

  @Override
  public long term() {
    return term;
  }

  @Override
  public boolean voted() {
    return voted;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, term, voted);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LocalVoteResponse) {
      LocalVoteResponse response = (LocalVoteResponse) object;
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
  public static class Builder extends AbstractLocalResponse.Builder<VoteResponse.Builder, VoteResponse> implements VoteResponse.Builder {
    private long term;
    private boolean voted;

    @Override
    public Builder withTerm(long term) {
      this.term = Assert.argNot(term, term < 0, "term cannot be negative");
      return this;
    }

    @Override
    public Builder withVoted(boolean voted) {
      this.voted = voted;
      return this;
    }

    @Override
    public LocalVoteResponse build() {
      return new LocalVoteResponse(status, error, term, voted);
    }
  }
}
