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
package io.atomix.catalog.client;

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
     * Enforces causal (writes follow reads) command consistency.
     * <p>
     * Causal consistency guarantees consistency between non-overlapping commands from a single client. Commands that are submitted
     * concurrently by a single client are not guaranteed to be either applied to the Raft state machine in program order or
     * completed in program order. Note that if the client does not submit concurrent writes and instead blocks every command
     * {@link java.util.concurrent.CompletableFuture future}, {@code CAUSAL} consistency may be more performant since there is
     * no additional coordination involved.
     */
    CAUSAL,

    /**
     * Enforces linearizable (monotonic write) command consistency.
     * <p>
     * Linearizable consistency enforces sequential consistency for concurrent writes from a single client by sequencing
     * commands as they're applied to the Raft state machine. If a client submits writes <em>a</em>, <em>b</em>, and <em>c</em>
     * in that order, they're guaranteed to be applied to the Raft state machine and client {@link java.util.concurrent.CompletableFuture futures}
     * are guaranteed to be completed in that order.
     */
    LINEARIZABLE

  }

  /**
   * Returns the memory address of the command.
   *
   * @return The memory address of the command.
   */
  default long address() {
    return 0;
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
