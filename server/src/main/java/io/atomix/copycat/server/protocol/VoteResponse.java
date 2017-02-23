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
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.AbstractResponse;
import io.atomix.copycat.protocol.Response;

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

  /**
   * Returns a new vote response builder.
   *
   * @return A new vote response builder.
   */
  public static Builder builder() {
    return new Builder(new VoteResponse());
  }

  /**
   * Returns a vote response builder for an existing response.
   *
   * @param response The response to build.
   * @return The vote response builder.
   */
  public static Builder builder(VoteResponse response) {
    return new Builder(response);
  }

  private long term;
  private boolean voted;

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
  public void readObject(BufferInput buffer, Serializer serializer) {
    status = Response.Status.forId(buffer.readByte());
    if (status == Response.Status.OK) {
      error = null;
      term = buffer.readLong();
      voted = buffer.readBoolean();
    } else {
      error = CopycatError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Response.Status.OK) {
      buffer.writeLong(term).writeBoolean(voted);
    } else {
      buffer.writeByte(error.id());
    }
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
    return String.format("%s[status=%s, error=%s, term=%d, voted=%b]", getClass().getSimpleName(), status, error, term, voted);
  }

  /**
   * Poll response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, VoteResponse> {
    protected Builder(VoteResponse response) {
      super(response);
    }

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The vote response builder.
     * @throws IllegalArgumentException if {@code term} is negative
     */
    public Builder withTerm(long term) {
      response.term = Assert.argNot(term, term < 0, "term cannot be negative");
      return this;
    }

    /**
     * Sets whether the vote was granted.
     *
     * @param voted Whether the vote was granted.
     * @return The vote response builder.
     */
    public Builder withVoted(boolean voted) {
      response.voted = voted;
      return this;
    }

    /**
     * @throws IllegalStateException if {@code term} is negative
     */
    @Override
    public VoteResponse build() {
      super.build();
      if (response.status == Response.Status.OK) {
        Assert.stateNot(response.term < 0, "term cannot be negative");
      }
      return response;
    }

    @Override
    public int hashCode() {
      return Objects.hash(response);
    }

    @Override
    public boolean equals(Object object) {
      return object instanceof Builder && ((Builder) object).response.equals(response);
    }

    @Override
    public String toString() {
      return String.format("%s[response=%s]", getClass().getCanonicalName(), response);
    }

  }

}
