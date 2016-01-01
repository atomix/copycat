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
 * limitations under the License
 */
package io.atomix.copycat.client;

import java.time.Duration;

/**
 * Strategy for handling operation submission failures by a client.
 * <p>
 * When a client submits an operation to the cluster, if the operation fails to be committed due
 * to a communication or other failure, the retry strategy will be queried to determine how to
 * handle the operation failure.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface RetryStrategy {

  /**
   * Called when an operation attempt fails.
   *
   * @param attempt The operation attempt.
   * @param cause The cause of the failure.
   */
  void attemptFailed(Attempt attempt, Throwable cause);

  /**
   * Operation attempt.
   */
  interface Attempt {

    /**
     * Returns the attempt count.
     *
     * @return The attempt count.
     */
    int attempt();

    /**
     * Returns the attempted operation.
     *
     * @return The attempted operation.
     */
    Operation<?> operation();

    /**
     * Fails the operation.
     */
    void fail();

    /**
     * Fails the operation with a specific exception.
     *
     * @param t The exception with which to fail the operation.
     */
    void fail(Throwable t);

    /**
     * Immediately retries the operation.
     */
    void retry();

    /**
     * Retries the operation after the given duration.
     *
     * @param after The duration after which to retry the operation.
     */
    void retry(Duration after);

  }

}
