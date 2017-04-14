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

import io.atomix.copycat.server.storage.SegmentManager;
import io.atomix.copycat.server.storage.Storage;

import java.util.Collection;

/**
 * Manages a single compaction process.
 * <p>
 * Compaction managers are responsible for providing a set of {@link CompactionTask}s to be executed
 * during log compaction. Each {@link Compaction} type is associated with a compaction manager.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface CompactionManager {

  /**
   * Builds compaction tasks for the given segments.
   * <p>
   * The collection of compaction tasks will be run in parallel in a pool of
   * {@link Storage#compactionThreads()} background threads. Implementations should ensure that
   * individual tasks can be run in parallel by operating on different segments in the log.
   *
   * @param storage The storage configuration.
   * @param segments The segments for which to build compaction tasks.
   * @return An iterable of compaction tasks.
   */
  Collection<CompactionTask> buildTasks(Storage storage, SegmentManager segments);

}
