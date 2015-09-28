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
 * limitations under the License.
 */
package io.atomix.copycat.server;

import java.time.Instant;

import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Operation;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.session.Session;

/**
 * Represents the committed state and metadata of a Raft state machine operation.
 * <p>
 * When {@link Command commands} and {@link Query queries} are applied to the Raft {@link StateMachine}, they're
 * wrapped in a commit object. The commit object provides useful metadata regarding the location of the commit
 * in the Raft replicated log, the {@link #time()} at which the commit was logged, and the {@link Session} that
 * submitted the operation to the cluster.
 * <p>
 * When state machines are done using a commit object, users should always call either {@link #clean()} or {@link #close()}.
 * Failing to call either method is a bug, and Catalogue will log a warning message in such cases.
 *
 * @see Command
 * @see Query
 * @see Session
 * @see Instant
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Commit<T extends Operation> extends AutoCloseable {

  /**
   * Returns the commit index.
   * <p>
   * This is the index at which the committed {@link Operation} was written in the Raft log.
   * Catalog guarantees that this index will be the same for all instances of the given operation on all nodes in the
   * cluster.
   * <p>
   * Note that for {@link Query} operations, the returned {@code index} may actually represent
   * the last committed index in the Raft log since queries are not actually written to disk. This, however, does not
   * break the single index guarantee since queries will only be applied to the leader's {@link StateMachine}.
   *
   * @return The commit index.
   */
  long index();

  /**
   * Returns the session that submitted the operation.
   * <p>
   * The returned {@link Session} is representative of the session that submitted the operation
   * that resulted in this {@link Commit}. The session can be used to {@link Session#publish(String, Object)}
   * event messages to the client.
   *
   * @return The session that created the commit.
   */
  Session session();

  /**
   * Returns the time at which the operation was committed.
   * <p>
   * The time is representative of the time at which the leader wrote the operation to its log. Because instants
   * are replicated through the Raft consensus algorithm, they are guaranteed to be consistent across all servers
   * and therefore can be used to perform time-dependent operations such as expiring keys or timeouts.
   * <p>
   * Users should <em>never</em> use {@code System} time to control behavior in a state machine and should instead rely
   * upon {@link Commit} times for time-based controls.
   *
   * @return The commit time.
   */
  Instant time();

  /**
   * Returns the commit type.
   * <p>
   * This is the {@link java.lang.Class} returned by the committed operation's {@link Object#getClass()} method.
   *
   * @return The commit type.
   */
  Class<T> type();

  /**
   * Returns the operation submitted by the user.
   *
   * @return The operation submitted by the user.
   */
  T operation();

  /**
   * Cleans the commit from the underlying log.
   * <p>
   * When the commit is cleaned, it will be removed from the log and may be removed permanently from disk at some
   * arbitrary point in the future.
   */
  void clean();

  /**
   * Cleans the commit from the underlying log.
   * <p>
   * Optionally overrides the tombstone configuration for the command. For commits cleaned as tombstones, the
   * log cleaner will guarantee that previous related commits will be removed from the log prior to cleaning
   * the tombstone. This ensures consistency when replaying commit logs.
   *
   * @param tombstone Whether to clean the commit as a tombstone.
   */
  void clean(boolean tombstone);

  /**
   * Closes the commit.
   * <p>
   * Once the commit is closed, it may be recycled and should no longer be accessed by the closer.
   */
  @Override
  void close();

}
