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
package io.atomix.copycat.server;

import io.atomix.copycat.Command;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

/**
 * Support for periodically persisting {@link StateMachine} snapshots to disk.
 * <p>
 * When implemented by a {@link StateMachine}, this interface provides support for state machines to periodically
 * persist {@link io.atomix.copycat.server.storage.snapshot.Snapshot snapshots} to disk. As commands are replicated
 * and applied to the state machine, the underlying replicated log can grow unbounded. Copycat provides several
 * mechanisms for resolving the issues of growing logs. State machines can indicate when {@link Commit}s no longer
 * contribute to their state by {@link Commit#close() closing} unneeded commits, resulting in associated commands
 * being removed from the Raft log. However, for many use cases tracking the relevance of individual commits implies
 * significant overhead, and in some cases - such as with counters - it's entirely impractical. Snapshots provide an
 * alternative method for compacting logs by periodically writing the state machine state to disk and clearing the
 * underlying log.
 * <p>
 * To store a state machine's state, simply implement the {@link #snapshot(SnapshotWriter)} method and write the
 * complete state machine state to the snapshot via the {@link SnapshotWriter}. Copycat will periodically invoke
 * the method to take a new snapshot of the state machine's state when the underlying log rotates segments.
 * <p>
 * <pre>
 *   {@code
 *   public class MyStateMachine extends StateMachine implements Snapshottable {
 *     private long counter;
 *
 *     public void snapshot(SnapshotWriter writer) {
 *       writer.writeLong(counter);
 *     }
 *
 *     public long increment(Commit<Increment> commit) {
 *       counter++;
 *       commit.close();
 *     }
 *   }
 *   }
 * </pre>
 * {@link Command Command}s that contribute to a state machine's snapshot should be
 * marked with the {@link Command.CompactionMode#SNAPSHOT SNAPSHOT} compaction mode.
 * Snapshot commands will be stored in the underlying log until a snapshot has been taken of the state machine
 * state, at which time all {@code SNAPSHOT} commands up to the point in logical time at which the snapshot
 * was written will be marked for removal from disk.
 * <p>
 * <pre>
 *   {@code
 *   public class Increment implements Command<Long> {
 *     public CompactionMode compaction() {
 *       return CompactionMode.SNAPSHOT;
 *     }
 *   }
 *   }
 * </pre>
 * Snapshot writing is only one component of snapshotting. Snapshottable state machines must also be able to
 * recover from snapshots stored on disk after a failure. When a server recovers from an existing Raft log,
 * the log will be replayed to the state machine, and when a point in logical time (an {@code index}) at which
 * a snapshot was taken and persisted is reached, that snapshot will be applied to the state machine via the
 * {@link #install(SnapshotReader)} method.
 * <p>
 * <pre>
 *   {@code
 *   public class MyStateMachine extends StateMachine implements Snapshottable {
 *     private long counter;
 *
 *     public void install(SnapshotReader reader) {
 *       counter = reader.readLong();
 *     }
 *   }
 *   }
 * </pre>
 * Implementations of the {@link #install(SnapshotReader)} method should always read precisely what implementations
 * of {@link #snapshot(SnapshotWriter)} write. State machines can potentially use a mixture of {@code SNAPSHOT}
 * and other commands, and state machine implementations should take care not to overwrite non-snapshot command
 * state with snapshots. For simpler state machines, <em>users should use either snapshotting or log cleaning
 * but not both</em>.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Snapshottable {

  /**
   * Takes a snapshot of the state machine state.
   * <p>
   * This method will be called each time the underlying {@link io.atomix.copycat.server.storage.Log Log}
   * rotates segments. Once the snapshot has been written, the snapshot will be stored on disk and eventually
   * completed. Note that snapshots are normally not immediately completed upon completion of this method as
   * servers must wait for certain conditions to be met before persisting a snapshot. Therefore, state machines
   * should not assume that once a snapshot has been written that the state machine can or will recover from
   * that snapshot. Snapshot writers should also ensure that all snapshottable state machine state is written on
   * each snapshot. Typically, only the most recent snapshot is applied to a state machine upon recovery, so no
   * assumptions should be made about the persistence or retention of older snapshots.
   *
   * @param writer The snapshot writer.
   */
  void snapshot(SnapshotWriter writer);

  /**
   * Installs a snapshot of the state machine state.
   * <p>
   * This method will be called while a server is replaying its log at startup. Typically, only the most
   * recent snapshot of the state machine state will be installed upon log replay. State machines should recover
   * all snapshottable state machine state from an installed snapshot. Note, however, that depending on the
   * {@link Command.CompactionMode CompactionMode} of commands applied to a state machine,
   * snapshots may only represent a single component of state recovery. State machines that use a mixture of
   * snapshottable and cleanable state should not overwrite state resulting from other types of commands when a
   * snapshot is installed.
   *
   * @param reader The snapshot reader.
   */
  void install(SnapshotReader reader);

}
