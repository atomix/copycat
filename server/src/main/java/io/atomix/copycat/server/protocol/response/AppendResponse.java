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

import io.atomix.copycat.protocol.websocket.response.WebSocketResponse;
import io.atomix.copycat.server.transport.request.AppendRequest;

/**
 * Server append entries response.
 * <p>
 * Append entries responses are sent by followers to leaders to indicate whether the handling of
 * an {@link AppendRequest} was successful. Failed append entries
 * requests do not result in {@link WebSocketResponse.Status#ERROR} responses.
 * Instead, followers provide a successful response which indicates whether the append {@link #succeeded()}
 * and provides information regarding the follower's updated log to aid in resolving indexes on the leader.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AppendResponse extends RaftProtocolResponse {

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  long term();

  /**
   * Returns a boolean indicating whether the append was successful.
   *
   * @return Indicates whether the append was successful.
   */
  boolean succeeded();

  /**
   * Returns the last index of the replica's log.
   *
   * @return The last index of the responding replica's log.
   */
  long logIndex();

  /**
   * Append response builder.
   */
  interface Builder extends RaftProtocolResponse.Builder<Builder, AppendResponse> {

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The append response builder
     * @throws IllegalArgumentException if {@code term} is not positive
     */
    Builder withTerm(long term);

    /**
     * Sets whether the request succeeded.
     *
     * @param succeeded Whether the append request succeeded.
     * @return The append response builder.
     */
    Builder withSucceeded(boolean succeeded);

    /**
     * Sets the last index of the replica's log.
     *
     * @param index The last index of the replica's log.
     * @return The append response builder.
     * @throws IllegalArgumentException if {@code index} is negative
     */
    Builder withLogIndex(long index);
  }

}
