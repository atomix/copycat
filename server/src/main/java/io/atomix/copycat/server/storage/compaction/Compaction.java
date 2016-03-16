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
package io.atomix.copycat.server.storage.compaction;

/**
 * Log compaction type identifier.
 * <p>
 * Log compaction comes in two forms: {@link #MINOR} and {@link #MAJOR}. This enum provides identifiers
 * to indicate the log compaction type when {@link Compactor#compact(Compaction) compacting} the log.
 * Passing an explicit {@code Compaction} type will run the appropriate compaction process asynchronously.
 * <pre>
 *   {@code
 *   log.compactor().compact(Compaction.MAJOR);
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public enum Compaction {

  /**
   * Represents a minor compaction.
   * <p>
   * Minor compaction is the more efficient of the compaction processes which removes
   * {@link io.atomix.copycat.server.storage.Log#release(long) cleaned} non-tombstone entries from individual
   * {@link io.atomix.copycat.server.storage.Segment}s. See the {@link MinorCompactionTask} for more information.
   */
  MINOR {
    @Override
    CompactionManager manager(Compactor compactor) {
      return new MinorCompactionManager(compactor);
    }
  },

  /**
   * Represents a major compaction.
   * <p>
   * Major compaction is the more heavyweight process of removing all
   * {@link io.atomix.copycat.server.storage.Log#release(long) cleaned} entries that have been
   * {@link io.atomix.copycat.server.storage.Log#commit(long) committed} to the log and combining segment
   * files wherever possible. See the {@link MajorCompactionTask} for more information.
   */
  MAJOR {
    @Override
    CompactionManager manager(Compactor compactor) {
      return new MajorCompactionManager(compactor);
    }
  };

  /**
   * Returns the compaction manager for the compaction type.
   *
   * @return The compaction manager for the compaction type.
   */
  abstract CompactionManager manager(Compactor compactor);

  /**
   * Constants for specifying entry compaction modes.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  public enum Mode {

    /**
     * The {@code DEFAULT} compaction mode is a special compaction mode which indicates that the configured
     * default compaction mode should be applied to the command.
     */
    DEFAULT,

    /**
     * The {@code UNKNOWN} compaction mode is a special compaction mode which retains entries beyond the
     * snapshot index, requires that entries be stored on all servers, and removes entries from the log sequentially.
     */
    UNKNOWN,

    /**
     * The {@code SNAPSHOT} compaction mode indicates commands for which resulting state is stored in state machine
     * snapshots. Snapshot commands will be stored in the Raft log only until a snapshot of the state machine state has
     * been written to disk, at which time they'll be removed from the log. Note that snapshot commands can still safely
     * trigger state machine events. Commands that result in the publishing of events will be persisted in the log until
     * related events have been received by all clients even if a snapshot of the state machine has since been stored.
     */
    SNAPSHOT,

    /**
     * The {@code RELEASE} compaction mode retains the command in the log until it has been stored and applied
     * on a majority of servers in the cluster and is released.
     */
    RELEASE,

    /**
     * The {@code QUORUM} compaction mode retains the command in the log until it has been stored and applied
     * on a majority of servers in the cluster. Once the commit has been applied to the state machine and
     * released, it may be removed during minor or major compaction.
     */
    QUORUM,

    /**
     * The {@code FULL} compaction mode retains the command in the log until it has been stored and applied
     * on all servers in the cluster. Once the commit has been applied to a state machine and closed it may
     * be removed from the log during minor or major compaction.
     */
    FULL,

    /**
     * The {@code SEQUENTIAL} compaction mode retains the command in the log until it has been stored and
     * applied on all servers in the cluster. Once the commit has been applied to a state machine and closed,
     * it may be removed from the log <em>only during major compaction</em> to ensure that all prior completed
     * commits are removed first.
     */
    SEQUENTIAL,

    /**
     * The {@code EXPIRING} compaction mode is an alias of the {@link #SEQUENTIAL} mode. Expiring entries are
     * retained in the log until stored and applied on all servers. Once the commit has been applied to a state
     * machine and closed, it may be removed from the log <em>only during major compaction</em> to ensure that
     * all prior completed commits are removed first.
     */
    EXPIRING,

    /**
     * The {@code TOMBSTONE} compaction mode is an alias of the {@link #SEQUENTIAL} mode. Tombstone entries are
     * retained in the log until stored and applied on all servers. Once the commit has been applied to a state
     * machine and closed, it may be removed from the log <em>only during major compaction</em> to ensure that
     * all prior completed commits are removed first.
     */
    TOMBSTONE,

  }

}
