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

import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.server.session.Sessions;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import io.atomix.copycat.session.Session;
import io.atomix.copycat.util.Assert;

import java.time.Clock;

/**
 * Base class for user-provided Raft state machines.
 * <p>
 * Users should extend this class to create a state machine for use within a {@link CopycatServer}.
 * <p>
 * State machines are responsible for handling operations submitted to the Raft cluster and
 * filtering {@link Commit committed} operations out of the Raft log. The most important rule of state machines is
 * that <em>state machines must be deterministic</em> in order to maintain Copycat's consistency guarantees. That is,
 * state machines must not change their behavior based on external influences and have no side effects. Users should
 * <em>never</em> use {@code System} time to control behavior within a state machine.
 * <p>
 * When commands and queries (i.e. <em>operations</em>) are submitted to the Raft cluster,
 * the {@link CopycatServer} will log and replicate them as necessary and, once complete, apply them to the configured
 * state machine.
 *
 * <h3>Deterministic scheduling</h3>
 * The {@link StateMachineContext} is responsible for executing state machine operations sequentially and provides an
 * interface similar to that of {@link java.util.concurrent.ScheduledExecutorService} to allow state machines to schedule
 * time-based callbacks. Because of the determinism requirement, scheduled callbacks are guaranteed to be executed
 * deterministically as well. The context can be accessed via the {@link #context} field.
 * See the {@link StateMachineContext} documentation for more information.
 * <pre>
 *   {@code
 *   public void putWithTtl(Commit<PutWithTtl> commit) {
 *     map.put(commit.operation().key(), commit);
 *     executor.schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
 *       map.remove(commit.operation().key()).close();
 *     });
 *   }
 *   }
 * </pre>
 * <p>
 * During command or scheduled callbacks, {@link Sessions} can be used to send state machine events back to the client.
 * For instance, a lock state machine might use a client's {@link Session} to send a lock event to the client.
 * <pre>
 *   {@code
 *   public void unlock(Commit<Unlock> commit) {
 *     try {
 *       Commit<Lock> next = queue.poll();
 *       if (next != null) {
 *         next.session().publish("lock");
 *       }
 *     } finally {
 *       commit.close();
 *     }
 *   }
 *   }
 * </pre>
 * Attempts to {@link io.atomix.copycat.server.session.ServerSession#publish(String, Object) publish} events during the execution will result in an
 * {@link IllegalStateException}.
 * <p>
 * Even though state machines on multiple servers may appear to publish the same event, Copycat's protocol ensures that only
 * one server ever actually sends the event. Still, it's critical that all state machines publish all events to ensure
 * consistency and fault tolerance. In the event that a server fails after publishing a session event, the client will transparently
 * reconnect to another server and retrieve lost event messages.
 *
 * <h3>Log cleaning</h3>
 * As operations are logged, replicated, committed, and applied to the state machine, the underlying
 * {@link io.atomix.copycat.server.storage.Log} grows. Without freeing unnecessary commits from the log it would eventually
 * consume all available disk or memory. Copycat uses a log cleaning algorithm to remove {@link Commit}s that no longer
 * contribute to the state machine's state from the log. To aid in this process, it's the responsibility of state machine
 * implementations to indicate when each commit is no longer needed by calling {@link Commit#close()}.
 * <pre>
 *   {@code
 *   public class ValueStateMachine extends StateMachine {
 *     private Commit<SetValue> value;
 *
 *     public void set(Commit<SetValue> commit) {
 *       this.value = commit;
 *     }
 *
 *     public void delete(Commit<DeleteValue> commit) {
 *       if (value != null) {
 *         value.close();
 *         value = null;
 *       }
 *       commit.close();
 *     }
 *   }
 *   }
 * </pre>
 * State machines should hold on to the {@link Commit} object passed to operation callbacks for as long as the commit
 * contributes to the state machine's state. Once a commit is no longer needed, commits should be {@link Commit#close() closed}.
 * Closing a commit notifies the log compaction algorithm that it's safe to remove the commit from the internal
 * commit log. Copycat will guarantee that {@link Commit}s are persisted in the underlying
 * {@link io.atomix.copycat.server.storage.Log} as long as is necessary (even after a commit is closed) to
 * ensure all operations are applied to a majority of servers and to guarantee delivery of
 * {@link io.atomix.copycat.server.session.ServerSession#publish(String, Object) session events} published as a result of specific operations.
 * State machines only need to specify when it's safe to remove each commit from the log.
 * <p>
 * Note that if commits are not properly closed and are instead garbage collected, a warning will be logged.
 * Failure to {@link Commit#close() close} a command commit should be considered a critical bug since instances of the
 * command can eventually fill up disk.
 *
 * <h3>Snapshotting</h3>
 * On top of Copycat's log cleaning algorithm mentioned above, Copycat provides a mechanism for storing and loading
 * snapshots of a state machine's state. Snapshots are images of the state machine's state stored at a specific
 * point in logical time (an {@code index}). To support snapshotting, state machine implementations should implement
 * the {@link Snapshottable} interface.
 * <pre>
 *   {@code
 *   public class ValueStateMachine extends StateMachine implements Snapshottable {
 *     private Object value;
 *
 *     public void set(Commit<SetValue> commit) {
 *       this.value = commit.operation().value();
 *       commit.close();
 *     }
 *
 *     public void snapshot(SnapshotWriter writer) {
 *       writer.writeObject(value);
 *     }
 *   }
 *   }
 * </pre>
 * For snapshottable state machines, Copycat will periodically request a {@link io.atomix.copycat.server.storage.snapshot.Snapshot Snapshot}
 * of the state machine's state by calling the {@link Snapshottable#snapshot(SnapshotWriter)} method. Once the state
 * machine has written a snapshot of its state, Copycat will automatically remove all commands from the underlying log
 * marked with the {@link io.atomix.copycat.server.Commit.CompactionMode#SNAPSHOT SNAPSHOT} compaction mode. Note that
 * state machines should still ensure that snapshottable commits are {@link Commit#close() closed} once they've been
 * applied to the state machine, but state machines are free to immediately close all snapshottable commits.
 *
 * @see Commit
 * @see StateMachineContext
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class StateMachine implements AutoCloseable {
  protected StateMachineContext context;
  protected Clock clock;
  protected Sessions sessions;

  protected StateMachine() {
  }

  /**
   * Initializes the state machine.
   *
   * @param context The state machine context.
   * @throws NullPointerException if {@code context} is null
   */
  public void init(StateMachineContext context) {
    this.context = Assert.notNull(context, "context");
    this.clock = context.clock();
    this.sessions = context.sessions();
    if (this instanceof SessionListener) {
      context.sessions().addListener((SessionListener) this);
    }
  }

  /**
   * Applies a commit to the state machine.
   *
   * @param commit the commit to apply.
   * @return The commit result.
   */
  public abstract byte[] apply(Commit commit);

  /**
   * Closes the state machine.
   */
  @Override
  public void close() {
  }
}
