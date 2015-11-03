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
 * limitations under the License
 */
package io.atomix.copycat.server.storage.compaction;

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadPoolContext;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.SegmentManager;
import io.atomix.copycat.server.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Manages compaction of log {@link io.atomix.copycat.server.storage.Segment}s in a pool of background
 * threads.
 * <p>
 * The compactor is responsible for managing log compaction processes. Log {@link Compaction} processes
 * are run in a pool of background threads of the configured number of {@link Storage#compactionThreads()}.
 * {@link Compaction#MINOR} and {@link Compaction#MAJOR} executions are scheduled according to the configured
 * {@link Storage#minorCompactionInterval()} and {@link Storage#majorCompactionInterval()} respectively.
 * Compaction can also be run synchronously via {@link Compactor#compact()} or {@link Compactor#compact(Compaction)}.
 * <p>
 * When a {@link Compaction} is executed either synchronously or asynchronously, the compaction's associated
 * {@link CompactionManager} is called to build a list of {@link CompactionTask}s to run. Compaction tasks
 * are run in parallel in the compaction thread pool. However, the compactor will not allow multiple compaction
 * executions to run in parallel. If a compaction is attempted while another compaction is already running,
 * it will be ignored.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class Compactor implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(Compactor.class);
  private final Storage storage;
  private final Log log;
  private final SegmentManager segments;
  private final ScheduledExecutorService executor;
  private ScheduledFuture<?> minor;
  private ScheduledFuture<?> major;
  private CompletableFuture<Void> future;

  public Compactor(Storage storage, Log log, SegmentManager segments, ScheduledExecutorService executor) {
    this.storage = Assert.notNull(storage, "storage");
    this.log = Assert.notNull(log, "log");
    this.segments = Assert.notNull(segments, "segments");
    this.executor = Assert.notNull(executor, "executor");
    minor = executor.scheduleAtFixedRate(() -> compact(Compaction.MINOR), storage.minorCompactionInterval().toMillis(), storage.minorCompactionInterval().toMillis(), TimeUnit.MILLISECONDS);
    major = executor.scheduleAtFixedRate(() -> compact(Compaction.MAJOR), storage.majorCompactionInterval().toMillis(), storage.majorCompactionInterval().toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Compacts the log using the default {@link Compaction#MINOR} compaction strategy.
   *
   * @return A completable future to be completed once the log has been compacted.
   */
  public CompletableFuture<Void> compact() {
    return compact(Compaction.MINOR);
  }

  /**
   * Compacts the log using the given {@link CompactionManager}.
   * <p>
   * The provided {@link CompactionManager} will be queried for a list of {@link CompactionTask}s to run.
   * Tasks will be run in parallel in a pool of background threads, and the returned {@link CompletableFuture}
   * will be completed once those tasks have completed.
   *
   * @param compaction The compaction strategy.
   * @return A completable future to be completed once the log has been compacted.
   */
  public synchronized CompletableFuture<Void> compact(Compaction compaction) {
    if (future != null)
      return future;

    LOGGER.debug("Compacting log with compaction: {}", compaction);

    future = new CompletableFuture<>();

    ThreadContext compactorThread = ThreadContext.currentContext();

    CompactionManager manager = compaction.manager(this);
    AtomicInteger counter = new AtomicInteger();

    Collection<CompactionTask> tasks = manager.buildTasks(storage, log, segments);
    if (!tasks.isEmpty()) {
      LOGGER.debug("Executing {} compaction task(s)", tasks.size());
      for (CompactionTask task : tasks) {
        LOGGER.debug("Executing {}", task);
        ThreadContext taskThread = new ThreadPoolContext(executor, segments.serializer());
        taskThread.execute(task).whenComplete((result, error) -> {
          LOGGER.debug("{} complete", task);
          if (counter.incrementAndGet() == tasks.size()) {
            if (compactorThread != null) {
              compactorThread.executor().execute(() -> future.complete(null));
            } else {
              future.complete(null);
            }
          }
        });
      }
    } else {
      future.complete(null);
    }
    return future.whenComplete((result, error) -> future = null);
  }

  /**
   * Closes the log compactor.
   * <p>
   * When the compactor is closed, existing compaction tasks will be allowed to complete, future scheduled
   * compactions will be cancelled, and the underlying {@link ScheduledExecutorService} will be shut down.
   */
  @Override
  public void close() {
    if (minor != null)
      minor.cancel(false);
    if (major != null)
      major.cancel(false);
    executor.shutdown();
  }

}
