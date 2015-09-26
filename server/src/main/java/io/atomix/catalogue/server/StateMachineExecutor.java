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

package io.atomix.catalogue.server;

import io.atomix.catalogue.client.Operation;
import io.atomix.catalyst.util.concurrent.ThreadContext;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Facilitates registration and execution of state machine commands and provides deterministic scheduling.
 * <p>
 * The state machine executor is responsible for managing input to and output from a {@link StateMachine}.
 * When a state machine is started within a {@link RaftServer}, the state machine is
 * {@link StateMachine#configure(StateMachineExecutor) configured} with a state machine executor. The state machine
 * must register {@link io.atomix.catalogue.client.Command} and {@link io.atomix.catalogue.client.Query}
 * (operations) callbacks via the executor.
 * <pre>
 * {@code
 * public class MapStateMachine extends StateMachine {
 *   private final Map&lt;Object, Commit&lt;Put&gt;&gt; map = new HashMap&gt;&lt;();
 *
 *   @Override
 *   protected void configure(StateMachineExecutor executor) {
 *     executor.register(PutCommand.class, this::put);
 *   }
 *
 *   private Object put(Commit&lt;Put&gt; commit) {
 *     return map.put(commit.operation().key(), commit);
 *   }
 * }
 * }
 * </pre>
 * As operations are committed to the Raft log, the executor is responsible for applying them to the state machine.
 * {@link io.atomix.catalogue.client.Command commands} are guaranteed to be applied to the state machine in the order in which
 * they appear in the Raft log and always in the same thread, so state machines don't have to be thread safe.
 * {@link io.atomix.catalogue.client.Query queries} are not generally written to the Raft log and will instead be applied
 * according to their {@link io.atomix.catalogue.client.Query.ConsistencyLevel}.
 * <p>
 * State machines can use the executor to provide deterministic scheduling during command operations.
 * <pre>
 *   {@code
 *   private Object putWithTtl(Commit&lt;PutWithTtl&gt; commit) {
 *     map.put(commit.operation().key(), commit);
 *     executor().schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
 *       map.remove(commit.operation().key());
 *       commit.clean();
 *     });
 *   }
 *   }
 * </pre>
 * As with all state machine callbacks, the executor will ensure scheduled callbacks are executed sequentially and
 * deterministically. As long as state machines schedule callbacks deterministically, callbacks will be executed
 * deterministically. Internally, the state machine executor triggers callbacks based on various timestamps in the
 * Raft log. This means the scheduler is dependent on internal or user-defined operations being written to the log.
 * Prior to the execution of a command, any expired scheduled callbacks will be executed based on the command's
 * logged timestamp.
 * <p>
 * It's important to note that callbacks can only be scheduled during {@link io.atomix.catalogue.client.Command}
 * operations or by recursive scheduling. If a state machine attempts to schedule a callback via the executor
 * during the execution of a {@link io.atomix.catalogue.client.Query}, a {@link IllegalStateException} will be
 * thrown. This is because queries are usually only applied on a single state machine within the cluster, and
 * so scheduling callbacks in reaction to query execution would not be deterministic.
 *
 * @see StateMachine
 * @see StateMachineContext
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface StateMachineExecutor extends ThreadContext {

  /**
   * Returns the state machine context.
   * <p>
   * The context is reflective of the current position and state of the Raft state machine. In particular,
   * it exposes the current approximate {@link StateMachineContext#now() time} and all open
   * {@link io.atomix.catalogue.server.session.Sessions}.
   *
   * @return The state machine context.
   */
  StateMachineContext context();

  /**
   * Registers a global operation callback.
   *
   * @param callback The operation callback.
   * @return The state machine executor.
   * @throws NullPointerException if {@code callback} is null
   */
  StateMachineExecutor register(Function<Commit<? extends Operation<?>>, ?> callback);

  /**
   * Registers a void operation callback.
   *
   * @param type The operation type.
   * @param callback The operation callback.
   * @param <T> The operation type.
   * @return The state machine executor.
   * @throws NullPointerException if {@code type} or {@code callback} are null
   */
  <T extends Operation<Void>> StateMachineExecutor register(Class<T> type, Consumer<Commit<T>> callback);

  /**
   * Registers an operation callback.
   *
   * @param type The operation type.
   * @param callback The operation callback.
   * @param <T> The operation type.
   * @return The state machine executor.
   * @throws NullPointerException if {@code type} or {@code callback} are null
   */
  <T extends Operation<U>, U> StateMachineExecutor register(Class<T> type, Function<Commit<T>, U> callback);

  @Override
  default void close() {

  }

}
