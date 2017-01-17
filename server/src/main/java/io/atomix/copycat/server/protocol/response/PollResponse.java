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
 * Server poll response.
 * <p>
 * Poll responses are sent by active servers in response to poll requests by followers to indicate
 * whether the responding server would vote for the requesting server if it were a candidate. This is
 * indicated by the {@link #accepted()} field of the response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface PollResponse extends RaftProtocolResponse {

  /**
   * Returns the responding node's current term.
   *
   * @return The responding node's current term.
   */
  long term();

  /**
   * Returns a boolean indicating whether the poll was accepted.
   *
   * @return Indicates whether the poll was accepted.
   */
  boolean accepted();

  /**
   * Poll response builder.
   */
  interface Builder extends RaftProtocolResponse.Builder<Builder, PollResponse> {

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The poll response builder.
     * @throws IllegalArgumentException if {@code term} is not positive
     */
    Builder withTerm(long term);

    /**
     * Sets whether the poll was granted.
     *
     * @param accepted Whether the poll was granted.
     * @return The poll response builder.
     */
    Builder withAccepted(boolean accepted);
  }

}
