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

import io.atomix.copycat.Command;
import io.atomix.copycat.Operation;
import io.atomix.copycat.Query;
import io.atomix.copycat.error.CommandException;
import io.atomix.copycat.error.QueryException;
import io.atomix.copycat.session.Session;

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
   * Handles the failure of an operation submission attempt.
   * <p>
   * Retry strategies can choose how to react to the failure to submit an operation by either
   * {@link Attempt#fail() failing} or {@link Attempt#retry() retrying} the attempt. In the event that
   * an operation attempt is failed, the operation's associated {@link java.util.concurrent.CompletableFuture}
   * will be completed exceptionally.
   *
   * @param attempt The operation attempt.
   * @param cause The cause of the failure.
   */
  void attemptFailed(Attempt attempt, Throwable cause);

  /**
   * Represents a single attempt to submit an operation to the Copycat cluster.
   * <p>
   * For each operation submission attempt, an {@code Attempt} object will be provided to the
   * {@link RetryStrategy} upon failure.
   */
  interface Attempt {

    /**
     * Returns the attempt count.
     * <p>
     * The attempt count is representative of the number of submission attempts have been made
     * prior to the failure of this attempt. For the first {@link Attempt} for any given submitted
     * operation, the attempt count will be {@code 1} and will increase for each {@link #retry() retry}
     * of an attempt.
     *
     * @return The submission attempt count.
     */
    int attempt();

    /**
     * Returns the operation was attempted and failed.
     * <p>
     * The attempt operation is the {@link Command} or {@link Query} that was submitted by the user
     * to the {@link CopycatClient client}.
     *
     * @return The attempted operation.
     */
    Operation<?> operation();

    /**
     * Fails the operation.
     * <p>
     * When the operation is failed with no user-provided exception, the associated
     * {@link java.util.concurrent.CompletableFuture} will be
     * {@link java.util.concurrent.CompletableFuture#completeExceptionally(Throwable) completed exceptionally}
     * with either a {@link CommandException} or {@link QueryException}
     * based on the type of the attempted {@link #operation() operation}.
     */
    void fail();

    /**
     * Fails the operation with a specific exception.
     * <p>
     * When the operation is failed, the associated {@link java.util.concurrent.CompletableFuture} will
     * be {@link java.util.concurrent.CompletableFuture#completeExceptionally(Throwable) completed exceptionally}
     * with the provided exception.
     *
     * @param t The exception with which to fail the operation.
     */
    void fail(Throwable t);

    /**
     * Immediately retries the operation.
     * <p>
     * The operation will immediately be resubmitted to the cluster via the client's current
     * {@link Session Session}. If the client is in the
     * {@link CopycatClient.State#SUSPENDED SUSPENDED} state, the operation may be enqueued to be resubmitted
     * once the client reestablishes communication with the cluster. In the event that the client transitions between
     * the {@link CopycatClient.State#SUSPENDED} and {@link CopycatClient.State#CONNECTED} states, the operation
     * may be resubmitted under a new session.
     */
    void retry();

    /**
     * Retries the operation after the given duration.
     * <p>
     * The operation will be resubmitted to the cluster after the given duration has expired. Note that even
     * if operations are resubmitted after different durations, all operations will be completed in the order
     * in which they were submitted by the user. Reordering operations via attempt retries will not negatively
     * impact the order of results.
     * <p>
     * If the client is in the {@link CopycatClient.State#SUSPENDED SUSPENDED} state once the given {@link Duration}
     * expires, the operation may be enqueued to be resubmitted once the client reestablishes communication with
     * the cluster. In the event that the client transitions between the {@link CopycatClient.State#SUSPENDED}
     * and {@link CopycatClient.State#CONNECTED} states, the operation may be resubmitted under a new session.
     *
     * @param after The duration after which to retry the operation.
     */
    void retry(Duration after);

  }

}
