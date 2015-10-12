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

import io.atomix.copycat.server.storage.Segment;
import io.atomix.copycat.server.storage.SegmentManager;
import io.atomix.copycat.server.storage.Storage;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Builds tasks for the {@link Compaction#MAJOR} compaction process.
 * <p>
 * Major compaction works similarly to minor compaction in that the configured {@link Storage#majorCompactionInterval()}
 * dictates the interval at which the major compaction process runs. During major compaction, the
 * {@link io.atomix.copycat.server.storage.compaction.MajorCompactionManager} iterates through <em>all</em>
 * {@link io.atomix.copycat.server.storage.Log#commit(long) committed} segments and rewrites them sequentially with
 * all cleaned entries removed, including tombstones. This ensures that earlier segments are compacted before later
 * segments, and so stateful entries that were {@link io.atomix.copycat.server.storage.Log#clean(long) cleaned} prior
 * to related tombstones are guaranteed to be removed first.
 * <p>
 * As entries are removed from the log during minor and major compaction, log segment files begin to shrink. Copycat
 * does not want to have a thousand file pointers open, so some mechanism is required to combine segments as disk
 * space is freed. To that end, as the major compaction process iterates through the set of committed segments and
 * rewrites live entries, it combines multiple segments up to the configured segment capacity. When a segment becomes
 * full during major compaction, the compaction process rolls over to a new segment and continues compaction. This results
 * in a significantly smaller number of files.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MajorCompactionManager implements CompactionManager {

  @Override
  public List<CompactionTask> buildTasks(Storage storage, SegmentManager segments) {
    List<Segment> cleanableSegments = getCleanableSegments(segments);
    return !cleanableSegments.isEmpty() ? Collections.singletonList(new MajorCompactionTask(segments, cleanableSegments)) : Collections.EMPTY_LIST;
  }

  /**
   * Returns a list of cleanable log segments.
   *
   * @param manager The segment manager.
   * @return A list of cleanable log segments.
   */
  private List<Segment> getCleanableSegments(SegmentManager manager) {
    return manager.segments().stream().filter(s -> s.lastIndex() <= manager.commitIndex() && s.isFull()).collect(Collectors.toList());
  }

}
