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
package io.atomix.copycat.server.protocol.request;

/**
 * Server poll request.
 * <p>
 * Poll requests aid in the implementation of the so-called "pre-vote" protocol. They are sent by followers
 * to all other servers prior to transitioning to the candidate state. This helps ensure that servers that
 * can't win elections do not disrupt existing leaders when e.g. rejoining the cluster after a partition.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface PollRequest extends RaftProtocolRequest {

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  long term();

  /**
   * Returns the candidate's address.
   *
   * @return The candidate's address.
   */
  int candidate();

  /**
   * Returns the candidate's last log index.
   *
   * @return The candidate's last log index.
   */
  long logIndex();

  /**
   * Returns the candidate's last log term.
   *
   * @return The candidate's last log term.
   */
  long logTerm();

  /**
   * Poll request builder.
   */
  interface Builder extends RaftProtocolRequest.Builder<Builder, PollRequest> {

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The poll request builder.
     * @throws IllegalArgumentException if {@code term} is negative
     */
    Builder withTerm(long term);

    /**
     * Sets the request leader.
     *
     * @param candidate The request candidate.
     * @return The poll request builder.
     * @throws IllegalArgumentException if {@code candidate} is not positive
     */
    Builder withCandidate(int candidate);

    /**
     * Sets the request last log index.
     *
     * @param index The request last log index.
     * @return The poll request builder.
     * @throws IllegalArgumentException if {@code index} is negative
     */
    Builder withLogIndex(long index);

    /**
     * Sets the request last log term.
     *
     * @param term The request last log term.
     * @return The poll request builder.
     * @throws IllegalArgumentException if {@code term} is negative
     */
    Builder withLogTerm(long term);
  }

}
