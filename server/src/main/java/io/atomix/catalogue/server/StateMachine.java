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

import io.atomix.catalogue.client.Command;
import io.atomix.catalogue.client.Operation;
import io.atomix.catalogue.client.Query;
import io.atomix.catalogue.client.session.Session;
import io.atomix.catalogue.server.session.Sessions;
import io.atomix.catalyst.util.Assert;

import java.time.Clock;
import java.time.Instant;

/**
 * Base class for user-provided Raft state machines.
 * <p>
 * Users should extend this class to create a state machine for use within a {@link RaftServer}.
 * <p>
 * State machines are responsible for handling {@link Operation operations} submitted to the Raft
 * cluster and filtering {@link Commit committed} operations out of the Raft log. The most
 * important rule of state machines is that <em>state machines must be deterministic</em> in order to maintain Catalogue's
 * consistency guarantees. That is, state machines must not change their behavior based on external influences and have
 * no side effects. Users should <em>never</em> use {@code System} time to control behavior within a state machine.
 * <p>
 * When {@link Command commands} and {@link Query queries} (i.e. <em>operations</em>) are submitted
 * to the Raft cluster, the {@link RaftServer} will log and replicate them as necessary
 * and, once complete, apply them to the configured state machine.
 * <p>
 * Override the {@link #configure(StateMachineExecutor)} method to register state machine operations.
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
 * When operations are applied to the state machine they're wrapped in a {@link Commit} object. The commit provides the
 * context of how the command or query was committed to the cluster, including the log {@link Commit#index()}, the
 * {@link Session} from which the operation was submitted, and the approximate wall-clock {@link Commit#time()} at which
 * the commit was written to the Raft log. Note that the commit time is guaranteed to progress monotonically, but it may
 * not be representative of the progress of actual time. See the {@link Commit} documentation for more information.
 * <p>
 * During command or scheduled callbacks, {@link Sessions} can be used to send state machine events back to the client.
 * For instance, a lock state machine might use a client's {@link Session} to send a lock event to the client.
 * <pre>
 * {@code
 * public void unlock(Commit&lt;Unlock&gt; commit) {
 *   try {
 *     Commit&lt;Lock&gt; next = queue.poll();
 *     if (next != null) {
 *       next.session().publish("lock");
 *     }
 *   } finally {
 *     commit.clean();
 *   }
 * }
 * }
 * </pre>
 * State machine operations are guaranteed to be executed in the order in which they were submitted by the client,
 * always in the same thread, and thus always sequentially. State machines do not need to be thread safe, but they must
 * be deterministic. That is, state machines are guaranteed to see {@link Command}s in the same order on all servers,
 * and given the same commands in the same order, all servers' state machines should arrive at the same state.
 * <p>
 * The {@link StateMachineExecutor} is responsible for executing state machine operations sequentially and provides an
 * interface similar to that of {@link java.util.concurrent.ScheduledExecutorService} to allow state machines to schedule
 * time-based callbacks. Because of the determinism requirement, scheduled callbacks are guaranteed to be executed
 * deterministically as well. See the {@link StateMachineExecutor} documentation for more information.
 * <p>
 * As with other operations, state machines should ensure that the publishing of session events is deterministic.
 * Messages published via a {@link Session} will be managed according to the {@link io.atomix.catalogue.client.Command.ConsistencyLevel}
 * of the command being executed at the time the event was {@link Session#publish(String, Object) published}. Each command may
 * publish zero or many events. For events published during the execution of a {@link io.atomix.catalogue.client.Command.ConsistencyLevel#LINEARIZABLE}
 * command, the state machine executor will transparently await responses from the client(s) before completing the command.
 * For commands with lower consistency levels, command responses will be immediately sent. Session events are always guaranteed
 * to be received by the client in the order in which they were published by the state machine.
 * <p>
 * Even though state machines on multiple servers may appear to publish the same event, Catalogue's protocol ensures that only
 * one server ever actually sends the event. Still, it's critical that all state machines publish all events to ensure
 * consistency and fault tolerance. In the event that a server fails after publishing a session event, the client will transparently
 * reconnect to another server and retrieve lost event messages.
 * <p>
 * State machines should hold on to the {@link Commit} object passed to operation callbacks for as long as the commit
 * contributes to the state machine's state. Once a commit is no longer needed, {@link Query} commits should be
 * {@link Commit#close() closed} and {@link Command} commits should be {@link Commit#clean() cleaned}. Cleaning notifies
 * the log compaction algorithm that it's safe to remove the commit from the internal commit log. Note that if commits
 * are not properly cleaned from the log and is instead garbage collected, a warning will be logged.
 *
 * @see Commit
 * @see Command
 * @see Query
 * @see Session
 * @see StateMachineContext
 * @see StateMachineExecutor
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class StateMachine implements AutoCloseable {
  private StateMachineExecutor executor;

  protected StateMachine() {
  }

  /**
   * Initializes the state machine.
   *
   * @param executor The state machine executor.
   * @throws NullPointerException if {@code context} is null
   */
  public void init(StateMachineExecutor executor) {
    this.executor = Assert.notNull(executor, "executor");
    configure(executor);
  }

  /**
   * Configures the state machine.
   *
   * @param executor The state machine executor.
   */
  protected abstract void configure(StateMachineExecutor executor);

  /**
   * Returns the state machine executor.
   *
   * @return The state machine executor.
   */
  protected StateMachineExecutor executor() {
    return executor;
  }

  /**
   * Returns the state machine sessions.
   *
   * @return The state machine sessions.
   */
  protected Sessions sessions() {
    return executor.context().sessions();
  }

  /**
   * Returns the state machine's deterministic clock.
   *
   * @return The state machine's deterministic clock.
   */
  protected Clock clock() {
    return executor.context().clock();
  }

  /**
   * Returns the current state machine time.
   *
   * @return The current state machine time.
   */
  protected Instant now() {
    return executor.context().now();
  }

  /**
   * Called when a new session is registered.
   *
   * @param session The session that was registered.
   */
  public void register(Session session) {

  }

  /**
   * Called when a session is expired.
   *
   * @param session The expired session.
   */
  public void expire(Session session) {

  }

  /**
   * Called when a session is closed.
   *
   * @param session The session that was closed.
   */
  public void close(Session session) {

  }

  /**
   * Closes the state machine.
   */
  @Override
  public void close() {

  }

}
