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
 * remain deterministic, commands must provide {@link CompactionMode persistence levels}
 * to aid servers in deciding when it's safe to remove a command from the log. The persistence level allows state machines
 * to safely handle the complexities of removing state from state machines while ensuring state machines remain
 * deterministic, particularly in the event of a failure and replay of the commit log. See the
 * {@link CompactionMode} documentation for more info.
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
 * @see CompactionMode
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
   * Constants for specifying the command type.
   *
   * @see #type()
   */
  enum Type {

    /**
     * The {@code DEFAULT} command type is a special command type that covers all use cases according
     * to the system type.
     */
    DEFAULT {
      @Override
      public CompactionMode compaction() {
        return CompactionMode.DEFAULT;
      }
    },

    /**
     * The {@code UPDATE} command type indicates that the command is used to update system state and may
     * be retained in the state machine for an arbitrary amount of time.
     */
    UPDATE {
      @Override
      public CompactionMode compaction() {
        return CompactionMode.QUORUM;
      }
    },

    /**
     * The {@code EXPIRING} command type indicates that the command is used to update system state but may
     * ultimately expire after some arbitrary amount of time after which it will no longer contribute to
     * the system's state.
     */
    EXPIRING {
      @Override
      public CompactionMode compaction() {
        return CompactionMode.SEQUENTIAL;
      }
    },

    /**
     * The {@code DELETE} command type indicates that the command is used to delete system state.
     */
    DELETE {
      @Override
      public CompactionMode compaction() {
        return CompactionMode.SEQUENTIAL;
      }
    },

    /**
     * The {@code EVENT} command type is a special type used to indicate that the command triggers the
     * publishing of events but does not itself contribute to the system's state.
     */
    EVENT {
      @Override
      public CompactionMode compaction() {
        return CompactionMode.QUORUM;
      }
    };

    /**
     * Returns the compaction mode associated with this command type.
     *
     * @return The compaction mode associated with this command type.
     */
    public abstract CompactionMode compaction();

  }

  /**
   * Constants for specifying command compaction modes.
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
     */
    DEFAULT,

    /**
     * The {@code SNAPSHOT} compaction mode indicates commands for which resulting state is stored in state machine
     * snapshots. Snapshot commands will be stored in the Raft log only until a snapshot of the state machine state has
     * been written to disk, at which time they'll be removed from the log. Note that snapshot commands can still safely
     * trigger state machine events. Commands that result in the publishing of events will be persisted in the log until
     * related events have been received by all clients even if a snapshot of the state machine has since been stored.
     */
    SNAPSHOT,

    /**
     * The {@code QUORUM} compaction mode retains the command in the log until it has been stored on a majority
     * of servers in the cluster and has been applied to the state machine.
     */
    QUORUM,

    /**
     * The {@code FULL} compaction mode retains the command in the log until it has been stored and applied on
     * all servers in the cluster.
     */
    FULL,

    /**
     * The sequential compaction mode retains the command in the log until it has been stored and applied on
     * all servers and until all prior commands have been compacted from the log.
     */
    SEQUENTIAL,

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
   * Returns the command type.
   * <p>
   * The command type indicates how servers should handle persistence and replication of the command. Different
   * types of commands can be optimized based on the way they modify system state.
   *
   * @return The command type.
   */
  default Type type() {
    return Type.DEFAULT;
  }

  /**
   * Returns the command compaction mode.
   * <p>
   * The compaction mode will dictate how long the command is persisted in the Raft log before it can be compacted.
   *
   * @see CompactionMode
   *
   * @return The command compaction mode.
   */
  default CompactionMode compaction() {
    return type().compaction();
  }

}
