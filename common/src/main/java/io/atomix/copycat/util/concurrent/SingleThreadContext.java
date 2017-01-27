/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.copycat.util.concurrent;

import io.atomix.copycat.util.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Single threaded context.
 * <p>
 * This is a basic {@link ThreadContext} implementation that uses a
 * {@link ScheduledExecutorService} to schedule events on the context thread.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SingleThreadContext implements ThreadContext {
  private static final Logger LOGGER = LoggerFactory.getLogger(SingleThreadContext.class);
  private final ScheduledExecutorService executor;
  private volatile boolean blocked;
  private final Executor wrappedExecutor = new Executor() {
    @Override
    public void execute(Runnable command) {
      try {
        executor.execute(Runnables.logFailure(command, LOGGER));
      } catch (RejectedExecutionException e) {
      }
    }
  };

  /**
   * Creates a new single thread context.
   * <p>
   * The provided context name will be passed to {@link CopycatThreadFactory} and used
   * when instantiating the context thread.
   *
   * @param nameFormat The context nameFormat which will be formatted with a thread number.
   */
  public SingleThreadContext(String nameFormat) {
    this(new CopycatThreadFactory(nameFormat));
  }

  /**
   * Creates a new single thread context.
   *
   * @param factory The thread factory.
   */
  public SingleThreadContext(CopycatThreadFactory factory) {
    this(new ScheduledThreadPoolExecutor(1, factory));
  }

  /**
   * Creates a new single thread context.
   *
   * @param executor The executor on which to schedule events. This must be a single thread scheduled executor.
   */
  public SingleThreadContext(ScheduledExecutorService executor) {
    this(getThread(executor), executor);
  }

  public SingleThreadContext(Thread thread, ScheduledExecutorService executor) {
    this.executor = executor;
    Assert.state(thread instanceof CopycatThread, "not a Catalyst thread");
    ((CopycatThread) thread).setContext(this);
  }

  /**
   * Gets the thread from a single threaded executor service.
   */
  protected static CopycatThread getThread(ExecutorService executor) {
    final AtomicReference<CopycatThread> thread = new AtomicReference<>();
    try {
      executor.submit(() -> {
        thread.set((CopycatThread) Thread.currentThread());
      }).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to initialize thread state", e);
    }
    return thread.get();
  }

  @Override
  public void block() {
    this.blocked = true;
  }

  @Override
  public void unblock() {
    this.blocked = false;
  }

  @Override
  public boolean isBlocked() {
    return blocked;
  }

  @Override
  public Logger logger() {
    return LOGGER;
  }

  @Override
  public void execute(Runnable callback) {
    executor.execute(callback);
  }

  @Override
  public <T> CompletableFuture<T> execute(Supplier<T> callback) {
    CompletableFuture<T> future = new CompletableFuture<>();
    executor.execute(() -> {
      try {
        future.complete(callback.get());
      } catch (Throwable t) {
        future.completeExceptionally(t);
      }
    });
    return future;
  }

  @Override
  public Scheduled schedule(Duration delay, Runnable runnable) {
    ScheduledFuture<?> future = executor.schedule(Runnables.logFailure(runnable, LOGGER), delay.toMillis(), TimeUnit.MILLISECONDS);
    return () -> future.cancel(false);
  }

  @Override
  public Scheduled schedule(Duration delay, Duration interval, Runnable runnable) {
    ScheduledFuture<?> future = executor.scheduleAtFixedRate(Runnables.logFailure(runnable, LOGGER), delay.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
    return () -> future.cancel(false);
  }

  @Override
  public void close() {
    executor.shutdownNow();
  }

}
