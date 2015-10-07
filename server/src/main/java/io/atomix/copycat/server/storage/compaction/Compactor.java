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
import io.atomix.copycat.server.storage.SegmentManager;
import io.atomix.copycat.server.storage.Storage;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Log compactor.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class Compactor implements AutoCloseable {
  private final Storage storage;
  private final SegmentManager segments;
  private final ScheduledExecutorService executor;
  private ScheduledFuture<?> minor;
  private ScheduledFuture<?> major;
  private CompletableFuture<Void> future;

  public Compactor(Storage storage, SegmentManager segments, ScheduledExecutorService executor) {
    this.storage = Assert.notNull(storage, "storage");
    this.segments = Assert.notNull(segments, "segments");
    this.executor = Assert.notNull(executor, "executor");
    minor = executor.scheduleAtFixedRate(() -> compact(Compaction.MINOR), storage.minorCompactionInterval().toMillis(), storage.minorCompactionInterval().toMillis(), TimeUnit.MILLISECONDS);
    major = executor.scheduleAtFixedRate(() -> compact(Compaction.MAJOR), storage.majorCompactionInterval().toMillis(), storage.majorCompactionInterval().toMillis(), TimeUnit.MILLISECONDS);
  }

  /**
   * Compacts the log using the minor compaction strategy.
   *
   * @return A completable future to be completed once the log has been compacted.
   */
  public CompletableFuture<Void> compact() {
    return compact(Compaction.MINOR);
  }

  /**
   * Compacts the log using the given compaction strategy.
   *
   * @param compaction The compaction strategy.
   * @return A completable future to be completed once the log has been compacted.
   */
  public CompletableFuture<Void> compact(Compaction compaction) {
    if (future != null)
      return future;

    future = new CompletableFuture<>();

    ThreadContext compactorThread = ThreadContext.currentContext();

    CompactionManager manager = compaction.manager();
    AtomicInteger counter = new AtomicInteger();

    List<CompactionTask> tasks = manager.buildTasks(storage, segments);
    if (!tasks.isEmpty()) {
      for (CompactionTask task : tasks) {
        ThreadContext taskThread = new ThreadPoolContext(executor, segments.serializer());
        taskThread.execute(task).whenComplete((result, error) -> {
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

  @Override
  public void close() {
    if (minor != null)
      minor.cancel(false);
    if (major != null)
      major.cancel(false);
    executor.shutdown();
  }

}
