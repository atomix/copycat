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
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.ArrayList;
import java.util.List;

/**
 * Builds tasks for the {@link Compaction#MINOR} compaction process.
 * <p>
 * Minor compaction works by rewriting individual segments to remove entries where {@link Entry#isTombstone()}
 * is {@code false}. The minor compaction manager is responsible for building a list of {@link MinorCompactionTask}s
 * to compact segments. In the case of minor compaction, each task is responsible for compacting a single segment.
 * However, in order to ensure segments are not compacted without cause, this compaction manager attempts to
 * prioritize segments for which compaction will result in greater disk space savings.
 * <p>
 * Segments are selected for minor compaction based on several factors:
 * <ul>
 *   <li>The number of {@link Entry entries} in the segment that have been {@link Segment#clean(long) cleaned}</li>
 *   <li>The number of times the segment has been compacted already</li>
 * </ul>
 * <p>
 * Given the number of entries that have been cleaned from the segment, a percentage of entries reclaimed by
 * compacting the segment is calculated. Then, the percentage of entries that have been cleaned is multiplied
 * by the number of times the segment has been compacted. If the result of this calculation is greater than
 * the configured {@link Storage#compactionThreshold()} then the segment is selected for compaction.
 * <p>
 * The final formula is as follows:
 * <pre>
 *   {@code
 *   if ((segment.cleanCount() / (double) segment.count()) * segment.descriptor().version() > storage.compactionThreshold()) {
 *     // Compact the segment
 *   }
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class MinorCompactionManager implements CompactionManager {

  @Override
  public List<CompactionTask> buildTasks(Storage storage, SegmentManager segments) {
    List<CompactionTask> tasks = new ArrayList<>(segments.segments().size());
    for (Segment segment : getCleanableSegments(storage, segments)) {
      tasks.add(new MinorCompactionTask(segments, segment));
    }
    return tasks;
  }

  /**
   * Returns a list of compactable segments.
   *
   * @return A list of compactable segments.
   */
  private Iterable<Segment> getCleanableSegments(Storage storage, SegmentManager manager) {
    List<Segment> segments = new ArrayList<>();
    for (Segment segment : manager.segments()) {
      // Only allow compaction of segments that are full.
      if (segment.isCompacted() || (segment.isFull() && segment.lastIndex() <= manager.commitIndex())) {
        // Calculate the percentage of entries that have been marked for cleaning in the segment.
        double cleanPercentage = segment.cleanCount() / (double) segment.count();

        // If the percentage of entries marked for cleaning times the segment version meets the cleaning threshold,
        // add the segment to the segments list for cleaning.
        if (cleanPercentage * segment.descriptor().version() >= storage.compactionThreshold()) {
          segments.add(segment);
        }
      }
    }
    return segments;
  }

}
