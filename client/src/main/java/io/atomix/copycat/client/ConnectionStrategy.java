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
 * the event that a client's connection to the cluster is lost and the client must register a new session,
 * if recovering the client's session fails then the connection strategy will again dictate how to
 * handle the failure.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ConnectionStrategy {

  /**
   * Handles the failure of an attempt to connect to the cluster.
   * <p>
   * This method will be called for each failed attempt to connect to the cluster. Connection attempt
   * failures result when the client fails to connect to and establish a session with any known server
   * in the cluster. Thus, by the time this method is called the client will have attempted communication
   * with every server specified to the client.
   * <p>
   * Connection strategies can choose how to react to a failed attempt to connect to the cluster by
   * either {@link Attempt#retry() retrying} or {@link Attempt#fail() failing} the attempt. In the event
   * that a connection attempt is failed, the associated {@link java.util.concurrent.CompletableFuture}
   * will be completed exceptionally.
   *
   * @param attempt The failed attempt.
   */
  void attemptFailed(Attempt attempt);

  /**
   * Represents a failed attempt at connecting to the server.
   * <p>
   * For each attempt to connect to the cluster, a new {@code Attempt} object represents the attempt
   * to connect to and establish a session with any server in the cluster. In the event that an attempt
   * is {@link #retry() retried}, a new attempt will be created.
   */
  interface Attempt {

    /**
     * Returns the attempt count.
     * <p>
     * The attempt count is representative of the number of connection attempts have been made
     * prior to the failure of this attempt. For the first {@link Attempt}, the attempt count will be
     * {@code 1} and will increase for each {@link #retry() retry} of an attempt.
     *
     * @return The attempt count.
     */
    int attempt();

    /**
     * Fails the connection attempt.
     * <p>
     * When the attempt is failed with no user-provided exception, the associated
     * {@link java.util.concurrent.CompletableFuture} will be
     * {@link java.util.concurrent.CompletableFuture#completeExceptionally(Throwable) completed exceptionally}.
     */
    void fail();

    /**
     * Fails the connection attempt with the given exception.
     * <p>
     * When the attempt is failed, the associated {@link java.util.concurrent.CompletableFuture} will
     * be {@link java.util.concurrent.CompletableFuture#completeExceptionally(Throwable) completed exceptionally}
     * with the provided exception.
     *
     * @param error The exception with which to fail the attempt.
     */
    void fail(Throwable error);

    /**
     * Immediately retries connecting to the cluster.
     * <p>
     * The client will immediately reattempt to connect to the cluster and establish a new session.
     */
    void retry();

    /**
     * Retries connecting to the cluster after the given duration.
     * <p>
     * After the provided {@link Duration} has elapsed, the client will reattempt to connect to the
     * cluster and establish a new session.
     *
     * @param after The duration after which to retry the connect attempt.
     */
    void retry(Duration after);

  }

}
