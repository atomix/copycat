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
package io.atomix.copycat.client;

/**
 * Base interface for operations that modify system state.
 * <p>
 * Commands are submitted by clients to a Raft server and used to modify Raft cluster-wide state. When a command
 * is submitted to the cluster, if the command is received by a follower, the Raft protocol dictates that it must be
 * forwarded to the cluster leader. Once the leader receives a command, it logs and replicates the command to a majority
 * of the cluster before applying it to its state machine and responding with the result.
 * <p>
 * <b>Consistency levels</b>
 * <p>
 * Commands are allow both to modify system state and to trigger {@link io.atomix.copycat.client.session.Session#publish(String, Object) events}
 * published to client {@link io.atomix.copycat.client.session.Session sessions}. Whereas {@link Query} consistency
 * levels dictate when and how queries can be executed on follower nodes, command
 * {@link io.atomix.copycat.client.Command.ConsistencyLevel consistency levels} largely relate to how events triggered
 * by commands are handled. When a command is applied to a server state machine, the consistency level of the command
 * being executed dictates the requirements for handling the event. For instance, in a fictitious lock state machine,
 * one command might be the {@code UnlockCommand}. The unlock command would set the state machine state to {@code unlocked}
 * and perhaps send a message to any lock waiters notifying them that the lock is available. In this case,
 * {@link io.atomix.copycat.client.Command.ConsistencyLevel#LINEARIZABLE linearizable} consistency can be used to ensure
 * that lock holders are notified before the {@code unlock} operation completes.
 * <p>
 * <b>Persistence levels</b>
 * <p>
 * <em>Determinism is the number one rule of state machines!</em>
 * <p>
 * When commands are submitted to the Raft cluster, they're written to a commit log on disk or in memory (based on the
 * storage configuration) and replicated to a majority of the cluster. As disk usage grows over time, servers compact
 * their logs to remove commands that no longer contribute to the state machine's state. In order to ensure state machines
 * remain deterministic, commands must provide {@link io.atomix.copycat.client.Command.PersistenceLevel persistence levels}
 * to aid servers in deciding when it's safe to remove a command from the log. The persistence level allows state machines
 * to safely handle the complexities of removing state from state machines while ensuring state machines remain
 * deterministic, particularly in the event of a failure and replay of the commit log. See the
 * {@link io.atomix.copycat.client.Command.PersistenceLevel} documentation for more info.
 * <p>
 * <b>Serialization</b>
 * <p>
 * Commands must be serializable both by the client and by all servers in the cluster. By default, all operations use
 * Java serialization. However, default serialization in slow because it requires the full class name and does not allocate
 * memory efficiently. For this reason, it's recommended that commands implement {@link io.atomix.catalyst.serializer.CatalystSerializable}
 * or register a custom {@link io.atomix.catalyst.serializer.TypeSerializer} for better performance. Serializable types
 * can be registered in a {@code META-INF/services/io.atomix.catalyst.serializer.CatalystSerializable} service loader file
 * or on the associated client/server {@link io.atomix.catalyst.serializer.Serializer} instance.
 *
 * @see io.atomix.copycat.client.Command.ConsistencyLevel
 * @see io.atomix.copycat.client.Command.PersistenceLevel
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Command<T> extends Operation<T> {

  /**
   * Constants for specifying Raft {@link Command} consistency levels.
   * <p>
   * This enum provides identifiers for configuring consistency levels for {@link Command queries}
   * submitted to a Raft cluster.
   * <p>
   * Consistency levels are used to dictate how queries are routed through the Raft cluster and the requirements for
   * completing read operations based on submitted queries. For expectations of specific consistency levels, see below.
   *
   * @see #consistency()
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  enum ConsistencyLevel {

    /**
     * Enforces no command consistency.
     * <p>
     * Lack of consistency means that no guarantees are made with respect to when, how often, or in what order a command will
     * be applied to server state machines. Inconsistent commands require no coordination, but they may be applied more than
     * once or out of order.
     */
    NONE,

    /**
     * Enforces sequential command consistency.
     * <p>
     * All commands are applied to the server state machine in program order and at some point between their invocation and
     * response (linearization point). But session events related to commands can be controlled by this consistency level.
     * The sequential consistency level guarantees that all session events related to a command will be received by the client
     * in sequential order. However, it does not guarantee that the events will be received during the invocation of the command.
     */
    SEQUENTIAL,

    /**
     * Enforces linearizable command consistency.
     * <p>
     * Linearizable consistency enforces sequential consistency for concurrent writes from a single client by sequencing
     * commands as they're applied to the Raft state machine. If a client submits writes <em>a</em>, <em>b</em>, and <em>c</em>
     * in that order, they're guaranteed to be applied to the Raft state machine and client {@link java.util.concurrent.CompletableFuture futures}
     * are guaranteed to be completed in that order. Additionally, linearizable commands are guaranteed to be applied to the
     * server state machine some time between invocation and response, and command-related session events are guaranteed to be
     * received by clients prior to completion of the command.
     */
    LINEARIZABLE

  }

  /**
   * Constants for specifying Raft command persistence levels.
   * <p>
   * This enum provides constants for specifying how a command should be persisted in and ultimately removed from the
   * Raft log. All commands are written to the log and replicated, but in some cases certain commands need to be held
   * in the log until major compaction. This is the case with <em>tombstones</em>. Tombstones are commands that remove
   * state from the state machine. Such commands need to be handled with care in order to ensure the state machine remains
   * deterministic. If a tombstone command is removed from the log before any commands for which it removed state, the
   * state machine should be considered non-deterministic. <em>Determinism is the number one rule for state machines!</em>
   * <p>
   * Commands that remove state from the state machine should <em>always</em> be marked as {@link #PERSISTENT}. This
   * will ensure that the commands remain in the log until after any prior commands are removed. Note that in some cases
   * it may not be totally evident that a command needs to be marked persistent. For instance, in an imaginary map
   * state machine, a command that sets a key with a TTL (time-to-live) should be marked {@link #PERSISTENT} because,
   * while it contributes to the state machine's state for some time period, ultimately it removes state from the
   * state machine once it expires.
   * <p>
   * All commands that do not remove prior command state from a state machine can be safely marked {@link #EPHEMERAL}.
   *
   * @see #persistence()
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  enum PersistenceLevel {

    /**
     * Indicates that the command should be persisted in the Raft log until cleaned.
     * <p>
     * Most commands will be marked {@code EPHEMERAL}. Once a command has been marked for cleaning from the Raft log,
     * it may be removed from the log at any time
     * <p>
     * Some examples of ephemeral commands include:
     * <ul>
     *   <li>Setting a key in a map</li>
     *   <li>Adding a value to a set</li>
     *   <li>Acquiring a lock</li>
     *   <li>Joining a membership group</li>
     * </ul>
     * <p>
     * Be careful for potential error in commands that disappear from the state machine after some time interval,
     * such as setting a key in a map with a TTL (time-to-live). Commands that result in the removal of state machine
     * state should <em>always</em> be marked {@link #PERSISTENT} even if the removal happens at some arbitrary point
     * in the future.
     */
    EPHEMERAL,

    /**
     * Indicates the command should be persisted in the Raft log until all prior related commands have been cleaned.
     * <p>
     * The {@code PERSISTENT} persistence level is useful for things like tombstones. Tombstones are commands that
     * remove state from the state machine. They essentially represent the deletion of history, and as such they
     * must be persisted in the log until the history prior to the tombstone is physically deleted from disk
     * (or memory).
     * <p>
     * Some examples of persistent commands include:
     * <ul>
     *   <li>Setting a key in a map with a TTL (time-to-live)</li>
     *   <li>Releasing a lock</li>
     *   <li>Leaving a membership group</li>
     * </ul>
     * <p>
     * In the event of a failure and replay of the commit log, persistent commands are guaranteed to be replayed
     * to the server side state machine(s) as long as prior commands remain in the log.
     */
    PERSISTENT

  }

  /**
   * Returns the command consistency level.
   * <p>
   * The consistency will dictate the order with which commands are submitted to the Raft cluster. Ultimately, all commands
   * are linearized by Raft. But commands submitted concurrently by a single client may be received by the cluster out of order.
   * The consistency level allows users to specify how out-of-order commands should be handled. Consult the {@link ConsistencyLevel}
   * documentation for more information.
   * <p>
   * By default, this method enforces strong consistency with the {@link ConsistencyLevel#LINEARIZABLE} consistency level.
   *
   * @see ConsistencyLevel
   *
   * @return The command consistency level.
   */
  default ConsistencyLevel consistency() {
    return null;
  }

  /**
   * Returns the command persistence level.
   * <p>
   * The persistence level will dictate how long the command is persisted in the Raft log before it can be cleaned.
   *
   * @see PersistenceLevel
   *
   * @return The command persistence level.
   */
  default PersistenceLevel persistence() {
    return PersistenceLevel.EPHEMERAL;
  }

}
