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

import java.util.ArrayList;
import java.util.List;

/**
 * Builds tasks for the {@link Compaction#MINOR} compaction process.
 * <p>
 * Minor compaction is the more frequent and lightweight process. Periodically, according to the configured
 * {@link Storage#minorCompactionInterval()}, a background thread will evaluate the log for minor compaction. The
 * minor compaction process iterates through segments and selects compactable segments based on the ratio of entries
 * that have been {@link io.atomix.copycat.server.storage.Log#clean(long) cleaned}. Minor compaction is generational. The
 * {@link io.atomix.copycat.server.storage.compaction.MinorCompactionManager} is more likely to select recently written
 * segments than older segments. Once a set of segments have been compacted, for each segment a
 * {@link io.atomix.copycat.server.storage.compaction.MinorCompactionTask} rewrites the segment without cleaned entries.
 * This rewriting results in a segment with missing entries, and Copycat's Raft implementation accounts for that.
 * For instance, a segment with entries {@code {1, 2, 3}} can become {@code {1, 3}} after being cleaned, and any attempt
 * to {@link io.atomix.copycat.server.storage.Log#get(long) read} entry {@code 2} will result in a {@code null} entry.
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

        // If the segment is small enough that it can be combined with another segment then add it.
        if (segment.count() < segment.length() / 2) {
          segments.add(segment);
        } else {
          // Calculate the percentage of entries that have been marked for cleaning in the segment.
          double cleanPercentage = segment.cleanCount() / (double) segment.count();

          // If the percentage of entries marked for cleaning times the segment version meets the cleaning threshold,
          // add the segment to the segments list for cleaning.
          if (cleanPercentage * segment.descriptor().version() >= storage.compactionThreshold()) {
            segments.add(segment);
          }
        }
      }
    }
    return segments;
  }

}
