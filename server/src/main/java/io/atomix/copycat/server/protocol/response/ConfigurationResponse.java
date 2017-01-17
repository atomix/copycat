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
import io.atomix.copycat.server.cluster.Member;

import java.util.Collection;

/**
 * Server configuration response.
 * <p>
 * Configuration responses are sent in response to configuration change requests once a configuration
 * change is completed or fails. Note that configuration changes can frequently fail due to the limitation
 * of commitment of configuration changes. No two configuration changes may take place simultaneously. If a
 * configuration change is failed due to a conflict, the response status will be
 * {@link WebSocketResponse.Status#ERROR} but the response {@link #error()} will
 * be {@code null}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ConfigurationResponse extends RaftProtocolResponse {

  /**
   * Returns the response index.
   *
   * @return The response index.
   */
  long index();

  /**
   * Returns the configuration term.
   *
   * @return The configuration term.
   */
  long term();

  /**
   * Returns the response configuration time.
   *
   * @return The response time.
   */
  long timestamp();

  /**
   * Returns the configuration members list.
   *
   * @return The configuration members list.
   */
  Collection<Member> members();

  /**
   * Configuration response builder.
   */
  interface Builder<T extends Builder<T, U>, U extends ConfigurationResponse> extends RaftProtocolResponse.Builder<T, U> {

    /**
     * Sets the response index.
     *
     * @param index The response index.
     * @return The response builder.
     * @throws IllegalArgumentException if {@code index} is negative
     */
    T withIndex(long index);

    /**
     * Sets the response term.
     *
     * @param term The response term.
     * @return The response builder.
     * @throws IllegalArgumentException if {@code term} is negative
     */
    T withTerm(long term);

    /**
     * Sets the response time.
     *
     * @param time The response time.
     * @return The response builder.
     * @throws IllegalArgumentException if {@code time} is negative
     */
    T withTime(long time);

    /**
     * Sets the response members.
     *
     * @param members The response members.
     * @return The response builder.
     * @throws NullPointerException if {@code members} is null
     */
    T withMembers(Collection<Member> members);
  }

}
