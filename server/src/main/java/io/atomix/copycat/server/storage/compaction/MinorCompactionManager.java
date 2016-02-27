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
import io.atomix.copycat.server.storage.Segment;
import io.atomix.copycat.server.storage.SegmentManager;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Builds tasks for the {@link Compaction#MINOR} compaction process.
 * <p>
 * Minor compaction works by rewriting individual segments to remove entries that don't have to be removed sequentially
 * from the log. The minor compaction manager is responsible for building a list of {@link MinorCompactionTask}s
 * to compact segments. In the case of minor compaction, each task is responsible for compacting a single segment.
 * However, in order to ensure segments are not compacted without cause, this compaction manager attempts to
 * prioritize segments for which compaction will result in greater disk space savings.
 * <p>
 * Segments are selected for minor compaction based on several factors:
 * <ul>
 *   <li>The number of {@link Entry entries} in the segment that have been {@link Segment#release(long) released}</li>
 *   <li>The number of times the segment has been compacted already</li>
 * </ul>
 * <p>
 * Given the number of entries that have been released from the segment, a percentage of entries reclaimed by
 * compacting the segment is calculated. Then, the percentage of entries that have been released is multiplied
 * by the number of times the segment has been compacted. If the result of this calculation is greater than
 * the configured {@link Storage#compactionThreshold()} then the segment is selected for compaction.
 * <p>
 * The final formula is as follows:
 * <pre>
 *   {@code
 *   if ((segment.releaseCount() / (double) segment.count()) * segment.descriptor().version() > storage.compactionThreshold()) {
 *     // Compact the segment
 *   }
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class MinorCompactionManager implements CompactionManager {
  private final Compactor compactor;

  MinorCompactionManager(Compactor compactor) {
    this.compactor = Assert.notNull(compactor, "compactor");
  }

  @Override
  public List<CompactionTask> buildTasks(Storage storage, SegmentManager segments) {
    List<CompactionTask> tasks = new ArrayList<>(segments.segments().size());
    for (Segment segment : getCompactableSegments(storage, segments)) {
      tasks.add(new MinorCompactionTask(segments, segment, compactor.snapshotIndex(), compactor.majorIndex(), compactor.getDefaultCompactionMode()));
    }
    return tasks;
  }

  /**
   * Returns a list of compactable segments.
   *
   * @return A list of compactable segments.
   */
  private Iterable<Segment> getCompactableSegments(Storage storage, SegmentManager manager) {
    List<Segment> segments = new ArrayList<>(manager.segments().size());
    Iterator<Segment> iterator = manager.segments().iterator();
    Segment segment = iterator.next();
    while (iterator.hasNext()) {
      Segment nextSegment = iterator.next();

      // Segments that have already been compacted are eligible for compaction. For uncompacted segments, the segment must be full, consist
      // of entries less than the minorIndex, and a later segment with at least one committed entry must exist in the log. This ensures that
      // a non-empty entry always remains at the end of the log.
      if (segment.isCompacted() || (segment.isFull() && segment.lastIndex() < compactor.minorIndex() && nextSegment.firstIndex() <= manager.commitIndex() && !nextSegment.isEmpty())) {
        // Calculate the percentage of entries that have been released in the segment.
        double compactablePercentage = segment.releaseCount() / (double) segment.count();

        // If the percentage of entries released times the segment version meets the compaction threshold,
        // add the segment to the segments list for compaction.
        if (compactablePercentage * segment.descriptor().version() >= storage.compactionThreshold()) {
          segments.add(segment);
        }
      }

      segment = nextSegment;
    }
    return segments;
  }

}
