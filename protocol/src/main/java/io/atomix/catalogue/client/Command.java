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
package io.atomix.catalogue.client;

import io.atomix.catalyst.util.BuilderPool;

/**
 * Raft state commands modify system state.
 * <p>
 * Commands are submitted by clients to a Raft server and used to modify Raft cluster-wide state. The Raft
 * consensus protocol dictates that commands must be forwarded to the cluster leader and replicated to a majority of
 * followers before being applied to the cluster state. Thus, in contrast to {@link Command cueries},
 * commands are not dictated by different consistency levels.
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
   */
  enum PersistenceLevel {

    /**
     * Indicates the command should be persisted in the Raft log until all prior related commands have been cleaned.
     * <p>
     * The {@code EPHEMERAL} persistence level is useful for things like tombstones.
     */
    EPHEMERAL,

    /**
     * Indicates that the command should be persisted in the Raft log until overwritten by another related command.
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
   * @return The command consistency level.
   */
  default ConsistencyLevel consistency() {
    return ConsistencyLevel.LINEARIZABLE;
  }

  /**
   * Returns the command persistence level.
   * <p>
   * The persistence level will dictate how long the command is persisted in the Raft log before it can be cleaned.
   *
   * @return The command persistence level.
   */
  default PersistenceLevel persistence() {
    return PersistenceLevel.PERSISTENT;
  }

  /**
   * Returns the command group code.
   *
   * @return The command group code.
   */
  default int groupCode() {
    return 0;
  }

  /**
   * Returns a boolean value indicating whether the given command is part of the same group as this command.
   *
   * @param command The command to check.
   * @return Indicates whether the given command is part of the same group as this command.
   */
  default boolean groupEquals(Command command) {
    return true;
  }

  /**
   * Base builder for commands.
   */
  abstract class Builder<T extends Builder<T, U, V>, U extends Command<V>, V> extends Operation.Builder<T, U, V> {
    protected U command;

    protected Builder(BuilderPool<T, U> pool) {
      super(pool);
    }

    @Override
    protected void reset(U command) {
      super.reset(command);
      this.command = command;
    }

    @Override
    public U build() {
      close();
      return command;
    }
  }

}
