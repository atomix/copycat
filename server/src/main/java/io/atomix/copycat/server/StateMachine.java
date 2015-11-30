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

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Operation;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.session.Sessions;

import java.lang.reflect.*;
import java.time.Clock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Base class for user-provided Raft state machines.
 * <p>
 * Users should extend this class to create a state machine for use within a {@link CopycatServer}.
 * <p>
 * State machines are responsible for handling {@link Operation operations} submitted to the Raft cluster and
 * filtering {@link Commit committed} operations out of the Raft log. The most important rule of state machines is
 * that <em>state machines must be deterministic</em> in order to maintain Copycat's consistency guarantees. That is,
 * state machines must not change their behavior based on external influences and have no side effects. Users should
 * <em>never</em> use {@code System} time to control behavior within a state machine.
 * <p>
 * When {@link Command commands} and {@link Query queries} (i.e. <em>operations</em>) are submitted to the Raft cluster,
 * the {@link CopycatServer} will log and replicate them as necessary and, once complete, apply them to the configured
 * state machine.
 * <p>
 * <b>State machine operations</b>
 * <p>
 * State machine operations are implemented as methods on the state machine. Operations can be automatically detected
 * by the state machine during setup or can be explicitly registered by overriding the {@link #configure(StateMachineExecutor)}
 * method. Each operation method must take a single {@link Commit} argument for a specific operation type.
 * <pre>
 *   {@code
 *   public class MapStateMachine extends StateMachine {
 *
 *     public Object put(Commit<Put> commit) {
 *       Commit<Put> previous = map.put(commit.operation().key(), commit);
 *       if (previous != null) {
 *         try {
 *           return previous.operation().value();
 *         } finally {
 *           previous.clean();
 *         }
 *       }
 *       return null;
 *     }
 *
 *     public Object get(Commit<Get> commit) {
 *       try {
 *         Commit<Put> current = map.get(commit.operation().key());
 *         return current != null ? current.operation().value() : null;
 *       } finally {
 *         commit.close();
 *       }
 *     }
 *   }
 *   }
 * </pre>
 * When operations are applied to the state machine they're wrapped in a {@link Commit} object. The commit provides the
 * context of how the command or query was committed to the cluster, including the log {@link Commit#index()}, the
 * {@link Session} from which the operation was submitted, and the approximate wall-clock {@link Commit#time()} at which
 * the commit was written to the Raft log. Note that the commit time is guaranteed to progress monotonically, but it may
 * not be representative of the progress of actual time. See the {@link Commit} documentation for more information.
 * <p>
 * State machine operations are guaranteed to be executed in the order in which they were submitted by the client,
 * always in the same thread, and thus always sequentially. State machines do not need to be thread safe, but they must
 * be deterministic. That is, state machines are guaranteed to see {@link Command}s in the same order on all servers,
 * and given the same commands in the same order, all servers' state machines should arrive at the same state with the
 * same output (return value). The return value of each operation callback is the response value that will be sent back
 * to the client.
 * <p>
 * <b>Deterministic scheduling</b>
 * <p>
 * The {@link StateMachineExecutor} is responsible for executing state machine operations sequentially and provides an
 * interface similar to that of {@link java.util.concurrent.ScheduledExecutorService} to allow state machines to schedule
 * time-based callbacks. Because of the determinism requirement, scheduled callbacks are guaranteed to be executed
 * deterministically as well. The executor can be accessed via the {@link #executor()} accessor method.
 * See the {@link StateMachineExecutor} documentation for more information.
 * <pre>
 *   {@code
 *   public void putWithTtl(Commit<PutWithTtl> commit) {
 *     map.put(commit.operation().key(), commit);
 *     executor.schedule(Duration.ofMillis(commit.operation().ttl()), () -> {
 *       map.remove(commit.operation().key()).clean();
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
 *       commit.clean();
 *     }
 *   }
 *   }
 * </pre>
 * Attempts to {@link Session#publish(String, Object) publish} events during the execution will result in an
 * {@link IllegalStateException}.
 * <p>
 * As with other operations, state machines should ensure that the publishing of session events is deterministic.
 * Messages published via a {@link Session} will be managed according to the {@link Command.ConsistencyLevel}
 * of the command being executed at the time the event was {@link Session#publish(String, Object) published}. Each command may
 * publish zero or many events. For events published during the execution of a {@link Command.ConsistencyLevel#LINEARIZABLE}
 * command, the state machine executor will transparently await responses from the client(s) before completing the command.
 * For commands with lower consistency levels, command responses will be immediately sent. Session events are always guaranteed
 * to be received by the client in the order in which they were published by the state machine.
 * <p>
 * Even though state machines on multiple servers may appear to publish the same event, Copycat's protocol ensures that only
 * one server ever actually sends the event. Still, it's critical that all state machines publish all events to ensure
 * consistency and fault tolerance. In the event that a server fails after publishing a session event, the client will transparently
 * reconnect to another server and retrieve lost event messages.
 * <p>
 * <b>Cleaning commits</b>
 * <p>
 * As operations are logged, replicated, committed, and applied to the state machine, the underlying
 * {@link io.atomix.copycat.server.storage.Log} grows. Without freeing unnecessary commits from the log it would eventually
 * consume all available disk or memory. Copycat uses a log cleaning algorithm to remove {@link Commit}s that no longer
 * contribute to the state machine's state from the log. To aid in this process, it's the responsibility of state machine
 * implementations to indicate when each commit is no longer needed by calling {@link Commit#clean()}.
 * <p>
 * State machines should hold on to the {@link Commit} object passed to operation callbacks for as long as the commit
 * contributes to the state machine's state. Once a commit is no longer needed, {@link Query} commits should be
 * {@link Commit#close() closed} and {@link Command} commits should be {@link Commit#clean() cleaned}. Cleaning notifies
 * the log compaction algorithm that it's safe to remove the commit from the internal commit log. Copycat will guarantee
 * that {@link Commit}s are persisted in the underlying {@link io.atomix.copycat.server.storage.Log} as long as is
 * necessary (even after a commit is cleaned) to ensure all operations are applied to a majority of servers and to
 * guarantee delivery of {@link Session#publish(String, Object) session events} published as a result of specific
 * operations. State machines only need to specify when it's safe to remove each commit from the log.
 * <p>
 * Note that if commits are not properly cleaned from the log and are instead garbage collected, a warning will be logged.
 * Failure to {@link Commit#clean()} a {@link Command} commit from the log should be considered a critical bug since
 * instances of the command can eventually fill up disk.
 *
 * @see Commit
 * @see Command
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
   * <p>
   * By default, this method will configure state machine operations by extracting public methods with
   * a single {@link Commit} parameter via reflection. Override this method to explicitly register
   * state machine operations via the provided {@link StateMachineExecutor}.
   *
   * @param executor The state machine executor.
   */
  protected void configure(StateMachineExecutor executor) {
    registerOperations();
  }

  /**
   * Returns the state machine executor.
   * <p>
   * The executor can be used to register state machine {@link Operation operations} or to schedule
   * time-based callbacks.
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
   * Closes the state machine.
   */
  @Override
  public void close() {

  }

  /**
   * Registers operations for the class.
   */
  private void registerOperations() {
    Class<?> type = getClass();
    for (Method method : type.getMethods()) {
      if (isOperationMethod(method)) {
        registerMethod(method);
      }
    }
  }

  /**
   * Returns a boolean value indicating whether the given method is an operation method.
   */
  private boolean isOperationMethod(Method method) {
    Class<?>[] paramTypes = method.getParameterTypes();
    return paramTypes.length == 1 && paramTypes[0] == Commit.class;
  }

  /**
   * Registers an operation for the given method.
   */
  private void registerMethod(Method method) {
    Type genericType = method.getGenericParameterTypes()[0];
    Class<?> argumentType = resolveArgument(genericType);
    if (argumentType != null && Operation.class.isAssignableFrom(argumentType)) {
      registerMethod(argumentType, method);
    }
  }

  /**
   * Resolves the generic argument for the given type.
   */
  private Class<?> resolveArgument(Type type) {
    if (type instanceof ParameterizedType) {
      ParameterizedType paramType = (ParameterizedType) type;
      return resolveClass(paramType.getActualTypeArguments()[0]);
    } else if (type instanceof TypeVariable) {
      return resolveClass(type);
    } else if (type instanceof Class) {
      TypeVariable<?>[] typeParams = ((Class<?>) type).getTypeParameters();
      return resolveClass(typeParams[0]);
    }
    return null;
  }

  /**
   * Resolves the generic class for the given type.
   */
  private Class<?> resolveClass(Type type) {
    if (type instanceof Class) {
      return (Class<?>) type;
    } else if (type instanceof ParameterizedType) {
      return resolveClass(((ParameterizedType) type).getRawType());
    } else if (type instanceof WildcardType) {
      Type[] bounds = ((WildcardType) type).getUpperBounds();
      if (bounds.length > 0) {
        return (Class<?>) bounds[0];
      }
    }
    return null;
  }

  /**
   * Registers the given method for the given operation type.
   */
  private void registerMethod(Class<?> type, Method method) {
    Class<?> returnType = method.getReturnType();
    if (returnType == void.class || returnType == Void.class) {
      registerVoidMethod(type, method);
    } else {
      registerValueMethod(type, method);
    }
  }

  /**
   * Registers an operation with a void return value.
   */
  @SuppressWarnings("unchecked")
  private void registerVoidMethod(Class type, Method method) {
    executor.register(type, wrapVoidMethod(method));
  }

  /**
   * Wraps a void method.
   */
  private Consumer wrapVoidMethod(Method method) {
    return c -> {
      try {
        method.invoke(this, c);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new AssertionError();
      }
    };
  }

  /**
   * Registers an operation with a non-void return value.
   */
  @SuppressWarnings("unchecked")
  private void registerValueMethod(Class type, Method method) {
    executor.register(type, wrapValueMethod(method));
  }

  /**
   * Wraps a value method.
   */
  private Function wrapValueMethod(Method method) {
    return c -> {
      try {
        return method.invoke(this, c);
      } catch (IllegalAccessException | InvocationTargetException e) {
        throw new AssertionError();
      }
    };
  }

}
