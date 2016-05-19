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

import io.atomix.catalyst.util.reference.ReferenceCounted;
import io.atomix.copycat.Command;
import io.atomix.copycat.Operation;
import io.atomix.copycat.Query;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.session.Session;

import java.time.Instant;

/**
 * Represents the committed state and metadata of a Raft state machine operation.
 * <p>
 * Commits are representative of a log {@link io.atomix.copycat.server.storage.entry.Entry} that has been replicated
 * and committed via the Raft consensus algorithm.
 * When {@link Command commands} and {@link Query queries} are applied to the Raft {@link StateMachine}, they're
 * wrapped in a commit object. The commit object provides useful metadata regarding the location of the commit
 * in the Raft replicated log, the {@link #time()} at which the commit was logged, and the {@link Session} that
 * submitted the operation to the cluster.
 * <p>
 * All metadata exposed by this interface is backed by disk. The {@link #operation() operation} and its metadata is
 * guaranteed to be consistent for a given {@link #index() index} across all servers in the cluster.
 * <p>
 * When state machines are done using a commit object, users should release the commit by calling {@link #close()}.
 * This notifies Copycat that it's safe to remove the commit from the log as it no longer contributes to the state
 * machine's state. Copycat guarantees that a commit will be retained in the log and replicated as long as it is
 * held open by the state machine. Failing to call either method is a bug and will result in disk eventually filling
 * up, and Copycat will log a warning message in such cases.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Commit<T extends Operation> extends ReferenceCounted<Commit<T>> {

  /**
   * Returns the commit index.
   * <p>
   * This is the index at which the committed {@link Operation} was written in the Raft log.
   * Copycat guarantees that this index will be unique for {@link Command} commits and will be the same for all
   * instances of the given operation on all servers in the cluster.
   * <p>
   * For {@link Query} operations, the returned {@code index} may actually be representative of the last committed
   * index in the Raft log since queries are not actually written to disk. Thus, query commits cannot be assumed
   * to have unique indexes.
   *
   * @return The commit index.
   * @throws IllegalStateException if the commit is {@link #close() closed}
   */
  long index();

  /**
   * Returns the session that submitted the operation.
   * <p>
   * The returned {@link Session} is representative of the session that submitted the operation
   * that resulted in this {@link Commit}. The session can be used to {@link ServerSession#publish(String, Object)}
   * event messages to the client.
   *
   * @return The session that created the commit.
   * @throws IllegalStateException if the commit is {@link #close() closed}
   */
  ServerSession session();

  /**
   * Returns the time at which the operation was committed.
   * <p>
   * The time is representative of the time at which the leader wrote the operation to its log. Because instants
   * are replicated through the Raft consensus algorithm, they are guaranteed to be consistent across all servers
   * and therefore can be used to perform time-dependent operations such as expiring keys or timeouts. Additionally,
   * commit times are guaranteed to progress monotonically, never going back in time.
   * <p>
   * Users should <em>never</em> use {@code System} time to control behavior in a state machine and should instead rely
   * upon {@link Commit} times or use the {@link StateMachineExecutor} for time-based controls.
   *
   * @return The commit time.
   * @throws IllegalStateException if the commit is {@link #close() closed}
   */
  Instant time();

  /**
   * Returns the commit type.
   * <p>
   * This is the {@link java.lang.Class} returned by the committed operation's {@link Object#getClass()} method.
   *
   * @return The commit type.
   * @throws IllegalStateException if the commit is {@link #close() closed}
   */
  Class<T> type();

  /**
   * Returns the operation submitted by the client.
   *
   * @return The operation submitted by the client.
   * @throws IllegalStateException if the commit is {@link #close() closed}
   */
  T operation();

  /**
   * Returns the command submitted by the client.
   * <p>
   * This method is an alias for the {@link #operation()} method. It is intended to aid with clarity in code.
   * This method does <em>not</em> perform any type checking of the operation to ensure it is in fact a
   * {@link Command} object.
   *
   * @return The command submitted by the client.
   * @throws IllegalStateException if the commit is {@link #close() closed}
   */
  default T command() {
    return operation();
  }

  /**
   * Returns the query submitted by the client.
   * <p>
   * This method is an alias for the {@link #operation()} method. It is intended to aid with clarity in code.
   * This method does <em>not</em> perform any type checking of the operation to ensure it is in fact a
   * {@link Query} object.
   *
   * @return The query submitted by the client.
   * @throws IllegalStateException if the commit is {@link #close() closed}
   */
  default T query() {
    return operation();
  }

  /**
   * Acquires a reference to the commit.
   * <p>
   * Initially, all open commits have a single reference. Acquiring new references to a commit will
   * increase the commit's {@link #references() reference count} which is used to determine when it's
   * safe to recycle the {@link Commit} object and compact the underlying entry from the log. Acquired
   * references must be {@link #release() released} once the commit is no longer needed to free up
   * memory and disk space.
   *
   * @return The referenced commit.
   */
  @Override
  Commit<T> acquire();

  /**
   * Releases a reference to the commit.
   * <p>
   * If releasing the commit results in the {@link #references() reference count} decreasing to
   * {@code 0} then the commit will be released back to a pool and the log entry underlying the commit
   * may be compacted from the log. Once all references to the commit have been released it should
   * no longer be accessed.
   *
   * @return Indicates whether all references to the commit have been released.
   */
  @Override
  boolean release();

  /**
   * Returns the number of open references to the commit.
   *
   * @return The number of open references to the commit.
   */
  @Override
  int references();

  /**
   * Closes the commit, releasing all references.
   * <p>
   * Closing a commit will make it immediately available for compaction from the replicated log.
   * Once the commit is closed, it may be recycled and should no longer be accessed by the closer.
   */
  @Override
  void close();

}
