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

import io.atomix.copycat.server.session.Session;
import io.atomix.copycat.util.buffer.BufferInput;

import java.time.Instant;

/**
 * Represents the committed state and metadata of a Raft state machine operation.
 * <p>
 * Commits are representative of a log {@link io.atomix.copycat.server.storage.entry.Entry} that has been replicated
 * and committed via the Raft consensus algorithm.
 * When commands and queries are applied to the Raft {@link StateMachine}, they're
 * wrapped in a commit object. The commit object provides useful metadata regarding the location of the commit
 * in the Raft replicated log, the {@link #time()} at which the commit was logged, and the {@link io.atomix.copycat.server.session.Session} that
 * submitted the operation to the cluster.
 * <p>
 * All metadata exposed by this interface is backed by disk. The operation and its metadata is
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
public interface Commit {

  /**
   * Returns the commit type.
   *
   * @return The commit type.
   */
  Type type();

  /**
   * Returns the commit index.
   * <p>
   * This is the index at which the committed operation was written in the Raft log.
   * Copycat guarantees that this index will be unique for command commits and will be the same for all
   * instances of the given operation on all servers in the cluster.
   * <p>
   * For query operations, the returned {@code index} may actually be representative of the last committed
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
   * The returned {@link io.atomix.copycat.server.session.Session} is representative of the session that submitted the operation
   * that resulted in this {@link Commit}. The session can be used to {@link Session#publish(io.atomix.copycat.util.buffer.Buffer)}
   * event messages to the client.
   *
   * @return The session that created the commit.
   * @throws IllegalStateException if the commit is {@link #close() closed}
   */
  Session session();

  /**
   * Returns the time at which the operation was committed.
   * <p>
   * The time is representative of the time at which the leader wrote the operation to its log. Because instants
   * are replicated through the Raft consensus algorithm, they are guaranteed to be consistent across all servers
   * and therefore can be used to perform time-dependent operations such as expiring keys or timeouts. Additionally,
   * commit times are guaranteed to progress monotonically, never going back in time.
   * <p>
   * Users should <em>never</em> use {@code System} time to control behavior in a state machine and should instead rely
   * upon {@link Commit} times or use the {@link StateMachineContext} for time-based controls.
   *
   * @return The commit time.
   * @throws IllegalStateException if the commit is {@link #close() closed}
   */
  Instant time();

  /**
   * Returns the commit buffer.
   *
   * @return The commit buffer.
   */
  BufferInput buffer();

  /**
   * Marks the commit for compaction from the Raft log.
   *
   * @param mode The compaction mode with which to compact the commit.
   */
  void compact(CompactionMode mode);

  /**
   * Closes the commit.
   * <p>
   * When a commit is closed, it will not be compacted from the log.
   */
  void close();

  /**
   * Commit type.
   */
  enum Type {
    COMMAND,
    QUERY,
  }

  /**
   * Constants for specifying command compaction modes.
   * <p>
   * Compaction modes dictate how each command is removed from a Copycat server's internal logs. As commands
   * are submitted to the cluster, written to disk, and replicated, the replicated log can grow unbounded.
   * Command compaction modes allow servers to determine when it's safe to remove a command from the log.
   * Ultimately, it is the responsibility of the state machine and the command applied to it to indicate when
   * the command may be removed from the log. Typically, commands should not be removed from the log until
   * they either no longer contribute to the state machine's state or some other mechanism ensures that the
   * command's state will not be loss, such as is the case with snapshotting.
   * <p>
   * Commands to a Copycat state machine typically come in one of two flavors; commands are either compacted from
   * the log via snapshotting or log cleaning. Log cleaning is the process of removing commands from the log when
   * they no longer contribute to the state machine's state. Commands compacted via log cleaning are represented
   * by the {@link #QUORUM}, {@link #SEQUENTIAL}, and {@link #TOMBSTONE} compaction modes. These types
   * of commands are removed from the log in a manor consistent with the configured compaction mode.
   * <p>
   * Alternatively, the simpler mode of compaction is snapshotting. Snapshotted commands are indicated by the
   * {@link #SNAPSHOT} compaction mode. When a server takes a snapshot of a state machine, all commands applied
   * to the state machine up to the logical time at which the snapshot was taken may be removed from the log.
   * <p>
   * It's important to note that command compaction modes only take effect once a command applied to a state
   * machine has been released for compaction. State machines effectively manage commands applied to a state machine
   * like memory. It's always the responsibility of a state machine to indicate when a command can be safely
   * compacted according to its compaction mode by releasing the command back to the server's storage layer for
   * compaction. See the state machine documentation for more info.
   *
   * @see #compact(CompactionMode)
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  enum CompactionMode {

    /**
     * The {@code SNAPSHOT} compaction mode indicates commands for which resulting state is stored in state machine
     * snapshots. Snapshot commands will be stored in the Raft log only until a snapshot of the state machine state has
     * been written to disk, at which time they'll be removed from the log.
     * <p>
     * While commands with the {@code SNAPSHOT} compaction mode may be removed once a snapshot of the state machine state
     * has been taken, it's still safe for {@code SNAPSHOT} commands to trigger state machine events. In the event that
     * a command triggers an event to a client, servers will ensure that the command is maintained in the log until
     * all associated events have been received by clients. That is, commands will never be replaced by a snapshot prior to
     * events being received by clients. In the event of a failure and replay of the log, the state machine will always
     * see commands for which events have not been acknowledged.
     */
    SNAPSHOT,

    /**
     * The {@code RELEASE} compaction mode retains the command in the log until it has been stored on a majority
     * of servers in the cluster and has been released by the state machine.
     * <p>
     * This compaction mode typically represents normal writes to a state machine. Once a {@code RELEASE} command
     * has been applied on a majority of state machines and have been released from memory, the command may be
     * safely removed from the log. It is the state machine's responsibility to indicate when it's safe for a
     * {@code RELEASE} command to be removed from the log by explicitly releasing the command once it no longer
     * contributes to the state machine's state. For instance, when one write overwrites the state that resulted
     * from a previous write, the previous write can be safely released and removed from the log during compaction.
     */
    RELEASE,

    /**
     * The {@code QUORUM} compaction mode retains the command in the log until it has been stored on a majority
     * of servers in the cluster and has been applied to the state machine.
     * <p>
     * This compaction mode typically represents normal writes to a state machine. Once a {@code QUORUM} command
     * has been applied on a majority of state machines and have been released from memory, the command may be
     * safely removed from the log. It is the state machine's responsibility to indicate when it's safe for a
     * {@code QUORUM} command to be removed from the log by explicitly releasing the command once it no longer
     * contributes to the state machine's state. For instance, when one write overwrites the state that resulted
     * from a previous write, the previous write can be safely released and removed from the log during compaction.
     */
    QUORUM,

    /**
     * The sequential compaction mode retains the command in the log until it has been stored and applied on
     * all servers and until all prior commands have been compacted from the log.
     * <p>
     * The {@code SEQUENTIAL} compaction mode adds to the <em>full replication</em> requirement of the {@code FULL}
     * compaction mode to also require that commands be removed from the log <em>in sequential order</em>. Typically,
     * this compaction mode is used for so called <em>tombstone</em> commands. Sequential ordering is critical in
     * the handling of tombstones since they essentially represent the absence of state. A tombstone cannot be safely
     * removed from the log until all prior related entries have been removed. Compacting tombstones sequentially ensures
     * that any prior related commands will have been compacted from the log prior to the tombstone being removed.
     */
    SEQUENTIAL,

    /**
     * The expiring compaction mode is an alias for the {@link #SEQUENTIAL} compaction mode that is specifically intended
     * for expiring commands like TTLs and other time-based operations. Expiring commands will be retained in the log until
     * stored and applied on all servers and will only be removed from the log once all prior released entries have been
     * removed.
     * <p>
     * The {@code EXPIRING} compaction mode adds to the <em>full replication</em> requirement of the {@code FULL}
     * compaction mode to also require that commands be removed from the log <em>in sequential order</em>. Typically,
     * this compaction mode is used for so called <em>tombstone</em> commands. Sequential ordering is critical in
     * the handling of tombstones since they essentially represent the absence of state. A tombstone cannot be safely
     * removed from the log until all prior related entries have been removed. Compacting tombstones sequentially ensures
     * that any prior related commands will have been compacted from the log prior to the tombstone being removed.
     */
    EXPIRING,

    /**
     * The tombstone compaction mode is an alias for the {@link #SEQUENTIAL} compaction mode that is specifically intended
     * for tombstone commands. Tombstones will be retained in the log until stored and applied on all servers, and tombstones
     * will only be removed from the log once all prior released entries have been removed.
     * <p>
     * The {@code TOMBSTONE} compaction mode adds to the <em>full replication</em> requirement of the {@code FULL}
     * compaction mode to also require that commands be removed from the log <em>in sequential order</em>. Typically,
     * this compaction mode is used for so called <em>tombstone</em> commands. Sequential ordering is critical in
     * the handling of tombstones since they essentially represent the absence of state. A tombstone cannot be safely
     * removed from the log until all prior related entries have been removed. Compacting tombstones sequentially ensures
     * that any prior related commands will have been compacted from the log prior to the tombstone being removed.
     */
    TOMBSTONE,

  }
}
