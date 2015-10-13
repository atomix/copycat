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
import io.atomix.copycat.server.storage.SegmentDescriptor;
import io.atomix.copycat.server.storage.SegmentManager;
import io.atomix.copycat.server.storage.entry.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Major compaction task.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class MajorCompactionTask implements CompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(MajorCompactionTask.class);
  private final SegmentManager manager;
  private final List<List<Segment>> groups;
  private List<List<OffsetCleaner>> cleaners;

  MajorCompactionTask(SegmentManager manager, List<List<Segment>> groups) {
    this.manager = Assert.notNull(manager, "manager");
    this.groups = Assert.notNull(groups, "segments");
  }

  @Override
  public void run() {
    storeCleaners();
    compactGroups();
  }

  /**
   * Stores cleaned segment offsets.
   */
  private void storeCleaners() {
    // Iterate through the segments in reverse order to prevent race conditions in which entries in later
    // segments are cleaned after entries in earlier segments.
    cleaners = new ArrayList<>(groups.size());
    for (int i = groups.size() - 1; i >= 0; i--) {
      List<Segment> group = groups.get(i);
      List<OffsetCleaner> groupCleaners = new ArrayList<>(group.size());
      for (int j = group.size() - 1; j >= 0; j--) {
        groupCleaners.add(new OffsetCleaner(group.get(j).cleaner().bits().copy()));
      }
      Collections.reverse(groupCleaners);
      cleaners.add(groupCleaners);
    }
    Collections.reverse(cleaners);
  }

  /**
   * Compacts all compactable segments.
   */
  private void compactGroups() {
    for (int i = 0; i < groups.size(); i++) {
      List<Segment> group = groups.get(i);
      List<OffsetCleaner> groupCleaners = cleaners.get(i);
      Segment segment = compactGroup(group, groupCleaners);
      updateCleaned(group, groupCleaners, segment);
      deleteGroup(group);
      closeCleaners(groupCleaners);
    }
  }

  /**
   * Compacts a group.
   */
  private Segment compactGroup(List<Segment> segments, List<OffsetCleaner> cleaners) {
    // Get the first segment which contains the first index being cleaned. The clean segment will be written
    // as a newer version of the earliest segment being rewritten.
    Segment firstSegment = segments.iterator().next();

    // Create a clean segment with a newer version to which to rewrite the segment entries.
    Segment compactSegment = manager.createSegment(SegmentDescriptor.builder()
      .withId(firstSegment.descriptor().id())
      .withVersion(firstSegment.descriptor().version() + 1)
      .withIndex(firstSegment.descriptor().index())
      .withMaxSegmentSize(segments.stream().mapToLong(s -> s.descriptor().maxSegmentSize()).max().getAsLong())
      .withMaxEntries(segments.stream().mapToInt(s -> s.descriptor().maxEntries()).max().getAsInt())
      .build());

    compactGroup(segments, cleaners, compactSegment);

    // Replace the rewritten segments with the updated segment.
    manager.replaceSegments(segments, compactSegment);

    return compactSegment;
  }

  /**
   * Compacts segments in a group sequentially.
   *
   * @param segments The segments to compact.
   * @param compactSegment The compact segment.
   */
  private void compactGroup(List<Segment> segments, List<OffsetCleaner> cleaners, Segment compactSegment) {
    // Iterate through all segments being compacted and write entries to a single compact segment.
    for (int i = 0; i < segments.size(); i++) {
      compactSegment(segments.get(i), cleaners.get(i), compactSegment);
    }
  }

  /**
   * Compacts the given segment.
   *
   * @param segment The segment to compact.
   * @param compactSegment The segment to which to write the compacted segment.
   */
  private void compactSegment(Segment segment, OffsetCleaner cleaner, Segment compactSegment) {
    for (long i = segment.firstIndex(); i <= segment.lastIndex(); i++) {
      compactEntry(i, segment, cleaner, compactSegment);
    }
  }

  /**
   * Compacts the entry at the given index.
   *
   * @param index The index at which to compact the entry.
   * @param segment The segment to compact.
   * @param compactSegment The segment to which to write the cleaned segment.
   */
  private void compactEntry(long index, Segment segment, OffsetCleaner cleaner, Segment compactSegment) {
    try (Entry entry = segment.get(index)) {
      // If an entry was found, remove the entry from the segment.
      if (entry != null) {
        // If the entry has been cleaned, skip the entry in the compact segment.
        // Note that for major compaction this process includes normal and tombstone entries.
        long offset = segment.offset(index);
        if (offset == -1 || cleaner.isClean(offset)) {
          compactSegment.skip(1);
          LOGGER.debug("Cleaned entry {} from segment {}", index, segment.descriptor().id());
        }
        // If the entry hasn't been cleaned, simply transfer it to the new segment.
        else {
          compactSegment.append(entry);
        }
      }
      // If the entry has already been compacted, skip the index in the segment.
      else {
        compactSegment.skip(1);
      }
    }
  }

  /**
   * Updates the new compact segment with entries that were cleaned during compaction.
   */
  private void updateCleaned(List<Segment> segments, List<OffsetCleaner> cleaners, Segment compactSegment) {
    for (int i = 0; i < segments.size(); i++) {
      updateCleanedOffsets(segments.get(i), cleaners.get(i), compactSegment);
    }
  }

  /**
   * Updates the new compact segment with entries that were cleaned in the given segment during compaction.
   */
  private void updateCleanedOffsets(Segment segment, OffsetCleaner cleaner, Segment compactSegment) {
    for (long i = segment.firstIndex(); i <= segment.lastIndex(); i++) {
      long offset = segment.offset(i);
      if (offset != -1 && cleaner.isClean(offset)) {
        compactSegment.clean(offset);
      }
    }
  }

  /**
   * Completes compaction by deleting old segments.
   */
  private void deleteGroup(List<Segment> group) {
    // Delete the old segments.
    for (Segment oldSegment : group) {
      oldSegment.delete();
    }
  }

  /**
   * Closes stored offset cleaners, freeing memory.
   */
  private void closeCleaners(List<OffsetCleaner> cleaners) {
    cleaners.forEach(OffsetCleaner::close);
    cleaners.clear();
  }

  @Override
  public String toString() {
    return String.format("%s[segments=%s]", getClass().getSimpleName(), groups);
  }

}
