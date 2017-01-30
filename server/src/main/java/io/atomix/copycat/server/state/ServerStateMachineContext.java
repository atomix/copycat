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

package io.atomix.copycat.server.state;

import io.atomix.copycat.server.StateMachineContext;
import io.atomix.copycat.util.Assert;
import io.atomix.copycat.util.concurrent.NonBlockingFuture;
import io.atomix.copycat.util.concurrent.Scheduled;
import io.atomix.copycat.util.concurrent.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/**
 * Server state machine context.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ServerStateMachineContext implements StateMachineContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerStateMachineContext.class);

  /**
   * Context type.
   */
  enum Type {
    COMMAND,
    QUERY,
  }

  private final ServerClock clock = new ServerClock();
  final ThreadContext executor;
  private final ServerSessionManager sessions;
  private Type type;
  private long index;
  private long timestamp;
  private final Queue<ServerTask> tasks = new ArrayDeque<>();
  private final List<ServerScheduledTask> scheduledTasks = new ArrayList<>();
  private final List<ServerScheduledTask> complete = new ArrayList<>();

  public ServerStateMachineContext(ThreadContext executor, ServerSessionManager sessions) {
    this.executor = executor;
    this.sessions = sessions;
  }

  /**
   * Initializes the context for a command.
   */
  void init(long index, Instant instant, Type type) {
    update(index, instant, type);
  }

  /**
   * Updates the state machine context.
   */
  void update(long index, Instant instant, Type type) {
    this.index = index;
    this.type = type;
    clock.set(instant);
  }

  /**
   * Returns the current context type.
   */
  Type type() {
    return type;
  }

  /**
   * Returns the current executor timestamp.
   *
   * @return The current executor timestamp.
   */
  long timestamp() {
    return timestamp;
  }

  /**
   * Returns an updated executor timestamp.
   *
   * @return The updated executor timestamp.
   */
  long timestamp(long timestamp) {
    this.timestamp = Math.max(this.timestamp, timestamp);
    return this.timestamp;
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public Clock clock() {
    return clock;
  }

  @Override
  public ServerSessionManager sessions() {
    return sessions;
  }

  @Override
  public Logger logger() {
    return executor.logger();
  }

  /**
   * Commits the application of a command to the state machine.
   */
  @SuppressWarnings("unchecked")
  void commit() {
    // Execute any tasks that were queue during execution of the command.
    if (!tasks.isEmpty()) {
      for (ServerTask task : tasks) {
        this.type = Type.COMMAND;
        try {
          if (task.future != null) {
            task.future.complete(task.callback.get());
          } else {
            task.callback.get();
          }
        } catch (Exception e) {
          if (task.future != null) {
            task.future.completeExceptionally(e);
          }
        }
      }
      tasks.clear();
    }

    long index = this.index;
    for (ServerSession session : sessions.sessions.values()) {
      session.commit(index);
    }
  }

  /**
   * Executes scheduled callbacks based on the provided time.
   */
  void tick(long index, long timestamp) {
    // Only create an iterator if there are actually tasks scheduled.
    if (!scheduledTasks.isEmpty()) {

      // Iterate through scheduled tasks until we reach a task that has not met its scheduled time.
      // The tasks list is sorted by time on insertion.
      Iterator<ServerScheduledTask> iterator = scheduledTasks.iterator();
      while (iterator.hasNext()) {
        ServerScheduledTask task = iterator.next();
        if (task.complete(timestamp)) {
          update(index, Instant.ofEpochMilli(task.time), ServerStateMachineContext.Type.COMMAND);
          task.execute();
          complete.add(task);
          iterator.remove();
        } else {
          break;
        }
      }

      // Iterate through tasks that were completed and reschedule them.
      for (ServerScheduledTask task : complete) {
        task.reschedule();
      }
      complete.clear();
    }
  }

  @Override
  public void checkThread() {
    executor.checkThread();
  }

  @Override
  public void execute(Runnable callback) {
    Assert.state(type == ServerStateMachineContext.Type.COMMAND, "callbacks can only be scheduled during command execution");
    tasks.add(new ServerTask(() -> {
      callback.run();
      return null;
    }, null));
  }

  @Override
  public <T> CompletableFuture<T> execute(Supplier<T> callback) {
    Assert.state(type == ServerStateMachineContext.Type.COMMAND, "callbacks can only be scheduled during command execution");
    CompletableFuture<T> future = new NonBlockingFuture<>();
    tasks.add(new ServerTask(callback, future));
    return future;
  }

  @Override
  public Scheduled schedule(Duration delay, Runnable callback) {
    Assert.state(type == ServerStateMachineContext.Type.COMMAND, "callbacks can only be scheduled during command execution");
    LOGGER.debug("Scheduled callback {} with delay {}", callback, delay);
    return new ServerScheduledTask(callback, delay.toMillis()).schedule();
  }

  @Override
  public Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback) {
    Assert.state(type == ServerStateMachineContext.Type.COMMAND, "callbacks can only be scheduled during command execution");
    LOGGER.debug("Scheduled repeating callback {} with initial delay {} and interval {}", callback, initialDelay, interval);
    return new ServerScheduledTask(callback, initialDelay.toMillis(), interval.toMillis()).schedule();
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, time=%s]", getClass().getSimpleName(), index, clock);
  }

  /**
   * Server task.
   */
  private static class ServerTask {
    private final Supplier callback;
    private final CompletableFuture future;

    private ServerTask(Supplier callback, CompletableFuture future) {
      this.callback = callback;
      this.future = future;
    }
  }

  /**
   * Scheduled task.
   */
  private class ServerScheduledTask implements Scheduled {
    private final long delay;
    private final long interval;
    private final Runnable callback;
    private long time;

    private ServerScheduledTask(Runnable callback, long delay) {
      this(callback, delay, 0);
    }

    private ServerScheduledTask(Runnable callback, long delay, long interval) {
      this.delay = delay;
      this.interval = interval;
      this.callback = callback;
      this.time = clock.instant().toEpochMilli() + delay;
    }

    /**
     * Schedules the task.
     */
    private Scheduled schedule() {
      // Perform binary search to insert the task at the appropriate position in the tasks list.
      if (scheduledTasks.isEmpty()) {
        scheduledTasks.add(this);
      } else {
        int l = 0;
        int u = scheduledTasks.size() - 1;
        int i;
        while (true) {
          i = (u + l) / 2;
          long t = scheduledTasks.get(i).time;
          if (t == time) {
            scheduledTasks.add(i, this);
            return this;
          } else if (t < time) {
            l = i + 1;
            if (l > u) {
              scheduledTasks.add(i + 1, this);
              return this;
            }
          } else {
            u = i - 1;
            if (l > u) {
              scheduledTasks.add(i, this);
              return this;
            }
          }
        }
      }
      return this;
    }

    /**
     * Reschedules the task.
     */
    private void reschedule() {
      if (interval > 0) {
        time = timestamp + interval;
        schedule();
      }
    }

    /**
     * Returns a boolean value indicating whether the task delay has been met.
     */
    private boolean complete(long timestamp) {
      return timestamp > time;
    }

    /**
     * Executes the task.
     */
    private synchronized void execute() {
      callback.run();
    }

    @Override
    public synchronized void cancel() {
      scheduledTasks.remove(this);
    }
  }

}
