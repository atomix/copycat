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
package io.atomix.copycat;

/**
 * Base interface for operations that modify system state.
 * <p>
 * Commands are submitted by clients to a Raft server and used to modify Raft cluster-wide state. When a command
 * is submitted to the cluster, if the command is received by a follower, the Raft protocol dictates that it must be
 * forwarded to the cluster leader. Once the leader receives a command, it logs and replicates the command to a majority
 * of the cluster before applying it to its state machine and responding with the result.
 * <h2>Compaction modes</h2>
 * <em>Determinism is the number one rule of state machines!</em>
 * <p>
 * When commands are submitted to the Raft cluster, they're written to a commit log on disk or in memory (based on the
 * storage configuration) and replicated to a majority of the cluster. As disk usage grows over time, servers compact
 * their logs to remove commands that no longer contribute to the state machine's state. In order to ensure state machines
 * remain deterministic, commands must provide {@link CompactionMode compaction modes}
 * to aid servers in deciding when it's safe to remove a command from the log. The compaction mode allows state machines
 * to safely handle the complexities of removing state from state machines while ensuring state machines remain
 * deterministic, particularly in the event of a failure and replay of the commit log. See the
 * {@link CompactionMode} documentation for more info.
 * <h3>Serialization</h3>
 * Commands must be serializable both by the client and by all servers in the cluster. By default, all operations use
 * Java serialization. However, default serialization in slow because it requires the full class name and does not allocate
 * memory efficiently. For this reason, it's recommended that commands implement {@link io.atomix.catalyst.serializer.CatalystSerializable}
 * or register a custom {@link io.atomix.catalyst.serializer.TypeSerializer} for better performance. Serializable types
 * can be registered on the associated client/server {@link io.atomix.catalyst.serializer.Serializer} instance.
 *
 * @see CompactionMode
 *
 * @param <T> command result type
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Command<T> extends Operation<T> {

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
   * by the {@link #QUORUM}, {@link #FULL}, {@link #SEQUENTIAL}, and {@link #TOMBSTONE} compaction modes. These types
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
   * @see #compaction()
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  enum CompactionMode {

    /**
     * The {@code DEFAULT} compaction mode is a special compaction mode which is dictated by the type of
     * system to which the command is being submitted. If the system's state machine supports snapshotting,
     * the command will be compacted via snapshots.
     * <p>
     * The compaction mode for {@code DEFAULT} commands is determined by whether the state machine is snapshottable.
     * Commands applied to snapshottable state machines will be cleaned from the log once a snapshot is taken after
     * the command is applied to the state machine.
     */
    DEFAULT,

    /**
     * The {@code UNKNOWN} compaction mode is a special compaction mode that behaves consistently for all
     * system types.
     * <p>
     * This compaction mode ensures that commands will only be removed from the log when safe to do so in all potential
     * cases. In practice, this means {@code UNKNOWN} commands will only be removed from the log once they have been
     * applied to the state machine and a snapshot of the state machine has been taken at some point after the command
     * was applied. Additionally, {@code UNKNOWN} commands will only be removed from the log during <em>major</em>
     * compaction to ensure that any prior related commands will have been removed as well. For this reason, this
     * compaction mode can be extremely inefficient as servers are able to compact their logs less frequently and
     * infrequent compactions require more disk I/O. <em>It is strongly recommended that commands provide specific
     * compaction modes</em> to improve compaction performance.
     */
    UNKNOWN,

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
     * The {@code FULL} compaction mode retains the command in the log until it has been stored and applied on
     * all servers in the cluster.
     * <p>
     * This compaction mode can be useful for cases where it's essential that a command be seen by <em>all</em>
     * servers in the cluster. Even if a {@code FULL} command is applied to a state machine and is subsequently
     * released by that state machine, servers will still ensure the command is replicated and applied to all state
     * machines in the cluster prior to removing the command from the log. Once the command has been applied to
     * and released by all state machines, it may be removed from server logs during compaction.
     * <p>
     * In cases where a new server is joining the cluster, the concept of <em>full replication</em> only applies
     * to commands committed to the cluster <em>after</em> the new server joined. In other words, if a new server {@code s}
     * joins at logical time {@code t} then commands with the {@code FULL} compaction mode committed after time {@code t + 1}
     * will be required to be stored and applied on server {@code s} to meet the requirements for full replication.
     */
    FULL,

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

  /**
   * Returns the command compaction mode.
   * <p>
   * The compaction mode will dictate the circumstances under which the command can be safely removed from the
   * Raft replicated log. Commands and the state machines to which they apply must coordinate the compaction process
   * via this mechanism.
   *
   * @see CompactionMode
   *
   * @return The command compaction mode.
   */
  default CompactionMode compaction() {
    return CompactionMode.DEFAULT;
  }

}
