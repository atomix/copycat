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
 * Connection strategies define how clients should handle initializing connections to the cluster.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ConnectionStrategy {

  /**
   * Called when an attempt to connect to the cluster fails.
   *
   * @param attempt The failed attempt.
   */
  void attemptFailed(Attempt attempt);

  /**
   * Represents a failed attempt at connecting to the server.
   */
  interface Attempt {

    /**
     * Indicates the number of attempts.
     *
     * @return The number of attempts.
     */
    int attempt();

    /**
     * Fails the connection attempt.
     */
    void fail();

    /**
     * Retries connecting to the cluster.
     */
    void retry();

    /**
     * Retries connecting to the cluster after the given duration.
     *
     * @param after The duration after which to retry the connect attempt.
     */
    void retry(Duration after);

  }

}
