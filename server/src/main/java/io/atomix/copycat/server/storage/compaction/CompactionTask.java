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

import io.atomix.copycat.server.storage.Storage;

/**
 * Performs compaction on a set of log {@link io.atomix.copycat.server.storage.Segment}s.
 * <p>
 * Compaction tasks are responsible for compacting one or more segments in a
 * {@link io.atomix.copycat.server.storage.Log}. Tasks are provided by a related {@link CompactionManager}
 * and are run in parallel in a pool of {@link Storage#compactionThreads()} background threads.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface CompactionTask extends Runnable {
}
