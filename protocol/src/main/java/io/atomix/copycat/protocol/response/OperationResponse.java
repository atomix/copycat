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
package io.atomix.copycat.protocol.response;

/**
 * Base client operation response.
 * <p>
 * All operation responses are sent with a {@link #result()} and the {@link #index()} (or index) of the state
 * machine at the point at which the operation was evaluated. The version allows clients to ensure state progresses
 * monotonically when switching servers by providing the state machine version in future operation requests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface OperationResponse extends SessionResponse {

  /**
   * Returns the operation index.
   *
   * @return The operation index.
   */
  long index();

  /**
   * Returns the event index.
   *
   * @return The event index.
   */
  long eventIndex();

  /**
   * Returns the operation result.
   *
   * @return The operation result.
   */
  Object result();

  /**
   * Operation response builder.
   */
  interface Builder<T extends Builder<T, U>, U extends OperationResponse> extends SessionResponse.Builder<T, U> {

    /**
     * Sets the response index.
     *
     * @param index The response index.
     * @return The response builder.
     * @throws IllegalArgumentException If the response index is not positive.
     */
    T withIndex(long index);

    /**
     * Sets the response index.
     *
     * @param eventIndex The response event index.
     * @return The response builder.
     * @throws IllegalArgumentException If the response index is not positive.
     */
    T withEventIndex(long eventIndex);

    /**
     * Sets the operation response result.
     *
     * @param result The response result.
     * @return The response builder.
     * @throws NullPointerException if {@code result} is null
     */
    T withResult(Object result);
  }

}
