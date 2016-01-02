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
 * <p>
 * Connection strategies are used by the {@link CopycatClient} at startup to indicate how to handle
 * failures when connecting to the cluster. When a client is first started, the client will attempt to
 * any server listed in the {@link io.atomix.catalyst.transport.Address address} list provided to the
 * client at startup. If the client fails to register a new session with any server, the connection
 * strategy will dictate how to handle the failure.
 * <p>
 * Connection strategies are used in the same manner when attempting to recover a lost session. In
 * the event that a client's connection to the cluster is lost and the client must open a new session,
 * if recovering the client's session fails then the connection strategy will again dictate how to
 * handle the failure.
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
     * Returns the attempt count.
     *
     * @return The attempt count.
     */
    int attempt();

    /**
     * Fails the connection attempt.
     */
    void fail();

    /**
     * Fails the connection attempt with the given exception.
     *
     * @param error The exception with which to fail the attempt.
     */
    void fail(Throwable error);

    /**
     * Immediately retries connecting to the cluster.
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
