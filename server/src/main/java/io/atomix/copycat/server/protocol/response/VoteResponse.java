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

/**
 * Server vote response.
 * <p>
 * Vote responses are sent by active servers in response to vote requests by candidate to indicate
 * whether the responding server voted for the requesting candidate. This is indicated by the
 * {@link #voted()} field of the response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface VoteResponse extends RaftProtocolResponse {

  /**
   * Returns the responding node's current term.
   *
   * @return The responding node's current term.
   */
  long term();

  /**
   * Returns a boolean indicating whether the vote was granted.
   *
   * @return Indicates whether the vote was granted.
   */
  boolean voted();

  /**
   * Poll response builder.
   */
  interface Builder extends RaftProtocolResponse.Builder<Builder, VoteResponse> {

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The vote response builder.
     * @throws IllegalArgumentException if {@code term} is negative
     */
    Builder withTerm(long term);

    /**
     * Sets whether the vote was granted.
     *
     * @param voted Whether the vote was granted.
     * @return The vote response builder.
     */
    Builder withVoted(boolean voted);
  }

}
