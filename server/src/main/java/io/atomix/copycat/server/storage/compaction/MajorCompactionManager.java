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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
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
  private final Compactor compactor;

  MajorCompactionManager(Compactor compactor) {
    this.compactor = Assert.notNull(compactor, "compactor");
  }

  @Override
  public List<CompactionTask> buildTasks(Storage storage, SegmentManager segments) {
    List<List<Segment>> groups = getCompactableGroups(storage, segments);
    return !groups.isEmpty() ? Collections.singletonList(new MajorCompactionTask(segments, groups, compactor.snapshotIndex(), compactor.majorIndex(), compactor.getDefaultCompactionMode())) : Collections.emptyList();
  }

  /**
   * Returns a list of segments lists to compact, where segments are grouped according to how they will be merged during
   * compaction.
   */
  public List<List<Segment>> getCompactableGroups(Storage storage, SegmentManager manager) {
    List<List<Segment>> compact = new ArrayList<>();
    List<Segment> segments = null;
    for (Segment segment : getCompactableSegments(manager)) {
      // If this is the first segment in a segments list, add the segment.
      if (segments == null) {
        segments = new ArrayList<>();
        segments.add(segment);
      }
      // If the total size of all segments is less than the maximum size of any segment, add the segment to the segments list.
      else if (segments.stream().mapToLong(Segment::size).sum() + segment.size() <= storage.maxSegmentSize()
        && segments.stream().mapToLong(Segment::count).sum() + segment.count() <= storage.maxEntriesPerSegment()) {
        segments.add(segment);
      }
      // If there's not enough room to combine segments, reset the segments list.
      else {
        compact.add(segments);
        segments = new ArrayList<>();
        segments.add(segment);
      }
    }

    // Ensure all compactable segments have been added to the compact segments list.
    if (segments != null) {
      compact.add(segments);
    }
    return compact;
  }

  /**
   * Returns a list of compactable log segments.
   *
   * @param manager The segment manager.
   * @return A list of compactable log segments.
   */
  private List<Segment> getCompactableSegments(SegmentManager manager) {
    List<Segment> segments = new ArrayList<>(manager.segments().size());
    Iterator<Segment> iterator = manager.segments().iterator();
    Segment segment = iterator.next();
    while (iterator.hasNext()) {
      Segment nextSegment = iterator.next();

      // Segments that have already been compacted are eligible for compaction. For uncompacted segments, the segment must be full, consist
      // of entries less than the minorIndex, and a later segment with at least one committed entry must exist in the log. This ensures that
      // a non-empty entry always remains at the end of the log.
      if (segment.isCompacted() || (segment.isFull() && segment.lastIndex() < compactor.minorIndex() && nextSegment.firstIndex() <= manager.commitIndex() && !nextSegment.isEmpty())) {
        segments.add(segment);
      } else {
        break;
      }

      segment = nextSegment;
    }
    return segments;
  }

}
