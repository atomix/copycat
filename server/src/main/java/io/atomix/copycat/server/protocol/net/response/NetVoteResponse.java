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
package io.atomix.copycat.server.protocol.net.response;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.net.response.AbstractNetResponse;
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
public class NetVoteResponse extends AbstractNetResponse implements VoteResponse, RaftNetResponse {
  private final long term;
  private final boolean voted;

  public NetVoteResponse(long id, Status status, CopycatError error, long term, boolean voted) {
    super(id, status, error);
    this.term = term;
    this.voted = voted;
  }

  @Override
  public Type type() {
    return RaftNetResponse.Types.VOTE_RESPONSE;
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
    if (object instanceof NetVoteResponse) {
      NetVoteResponse response = (NetVoteResponse) object;
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
  public static class Builder extends AbstractNetResponse.Builder<VoteResponse.Builder, VoteResponse> implements VoteResponse.Builder {
    private long term;
    private boolean voted;

    public Builder(long id) {
      super(id);
    }

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
    public NetVoteResponse build() {
      return new NetVoteResponse(id, status, error, term, voted);
    }
  }

  /**
   * Vote response serializer.
   */
  public static class Serializer extends AbstractNetResponse.Serializer<NetVoteResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetVoteResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.error == null) {
        output.writeByte(0);
      } else {
        output.writeByte(response.error.id());
      }
      output.writeLong(response.term);
      output.writeBoolean(response.voted);
    }

    @Override
    public NetVoteResponse read(Kryo kryo, Input input, Class<NetVoteResponse> type) {
      return new NetVoteResponse(input.readLong(), Status.forId(input.readByte()), CopycatError.forId(input.readByte()), input.readLong(), input.readBoolean());
    }
  }
}
