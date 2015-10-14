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
import java.util.Collections;
import java.util.List;

/**
 * Builds tasks for the {@link Compaction#MAJOR} compaction process.
 * <p>
 * Major compaction works by iterating through all committed {@link Segment}s in the log and rewriting and
 * combining segments to compact them together. Because of the sequential nature of major compaction, the major
 * compaction manager builds only a single {@link MajorCompactionTask} which will rewrite the log sequentially.
 * Segments are provided to the major compaction task in groups that indicate which segments to combine. A set
 * of segments can be combined if they meet the following criteria:
 * <ul>
 *   <li>The entries in the set of segments are sequential; there are no missing segments in the set
 *   such that combining the segments would result in a segment with missing entries</li>
 *   <li>The combined size of all segments in the set is less than the configured {@link Storage#maxSegmentSize()}</li>
 *   <li>The combined number of entries after compaction is less than the configured {@link Storage#maxEntriesPerSegment()}</li>
 * </ul>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class MajorCompactionManager implements CompactionManager {

  @Override
  public List<CompactionTask> buildTasks(Storage storage, SegmentManager segments) {
    List<List<Segment>> groups = getCleanableGroups(storage, segments);
    return !groups.isEmpty() ? Collections.singletonList(new MajorCompactionTask(segments, groups)) : Collections.emptyList();
  }

  /**
   * Returns a list of segment sets to clean.
   */
  private List<List<Segment>> getCleanableGroups(Storage storage, SegmentManager manager) {
    List<List<Segment>> clean = new ArrayList<>();
    List<Segment> segments = null;
    Segment previousSegment = null;
    for (Segment segment : getCleanableSegments(manager)) {
      // If this is the first segment in a segments list, add the segment.
      if (segments == null) {
        segments = new ArrayList<>();
        segments.add(segment);
      }
      // If the previous segment is undefined or of a different version, reset the segments.
      else if (previousSegment != null && previousSegment.descriptor().version() != segment.descriptor().version()) {
        clean.add(segments);
        segments = new ArrayList<>();
        segments.add(segment);
      }
      // If the total size of all segments is less than the maximum size of any segment, add the segment to the segments list.
      else if (segments.stream().mapToLong(Segment::size).sum() + segment.size() < storage.maxSegmentSize()
        && segments.stream().mapToLong(s -> s.count() - s.cleanCount()).sum() < storage.maxEntriesPerSegment()) {
        segments.add(segment);
      }
      // If there's not enough room to combine segments, reset the segments list.
      else {
        clean.add(segments);
        segments = new ArrayList<>();
        segments.add(segment);
      }
      previousSegment = segment;
    }

    // Ensure all cleanable segments have been added to the clean segments list.
    if (segments != null) {
      clean.add(segments);
    }
    return clean;
  }

  /**
   * Returns a list of cleanable log segments.
   *
   * @param manager The segment manager.
   * @return A list of cleanable log segments.
   */
  private List<Segment> getCleanableSegments(SegmentManager manager) {
    List<Segment> segments = new ArrayList<>(manager.segments().size());
    for (Segment segment : manager.segments()) {
      if (segment.lastIndex() <= manager.commitIndex() && (segment.isFull() || segment.isCompacted())) {
        segments.add(segment);
      } else {
        break;
      }
    }
    return segments;
  }

}
