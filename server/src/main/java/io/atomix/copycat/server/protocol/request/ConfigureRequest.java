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

import io.atomix.copycat.server.cluster.Member;

import java.util.Collection;

/**
 * Configuration installation request.
 * <p>
 * Configuration requests are special requests that aid in installing committed configurations
 * to passive and reserve members of the cluster. Prior to the start of replication from an active
 * member to a passive or reserve member, the active member must update the passive/reserve member's
 * configuration to ensure it is in the expected state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ConfigureRequest extends RaftProtocolRequest {

  /**
   * Returns the requesting node's current term.
   *
   * @return The requesting node's current term.
   */
  long term();

  /**
   * Returns the requesting leader address.
   *
   * @return The leader's address.
   */
  int leader();

  /**
   * Returns the configuration index.
   *
   * @return The configuration index.
   */
  long index();

  /**
   * Returns the configuration timestamp.
   *
   * @return The configuration timestamp.
   */
  long timestamp();

  /**
   * Returns the configuration members.
   *
   * @return The configuration members.
   */
  Collection<Member> members();

  /**
   * Heartbeat request builder.
   */
  interface Builder extends RaftProtocolRequest.Builder<Builder, ConfigureRequest> {

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code term} is not positive
     */
    Builder withTerm(long term);

    /**
     * Sets the request leader.
     *
     * @param leader The request leader.
     * @return The append request builder.
     * @throws IllegalArgumentException if the {@code leader} is not positive
     */
    Builder withLeader(int leader);

    /**
     * Sets the request index.
     *
     * @param index The request index.
     * @return The request builder.
     */
    Builder withIndex(long index);

    /**
     * Sets the request timestamp.
     *
     * @param timestamp The request timestamp.
     * @return The request builder.
     */
    Builder withTime(long timestamp);

    /**
     * Sets the request members.
     *
     * @param members The request members.
     * @return The request builder.
     * @throws NullPointerException if {@code member} is null
     */
    Builder withMembers(Collection<Member> members);
  }

}
