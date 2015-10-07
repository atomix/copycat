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

import java.util.List;

/**
 * Minor compaction task.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MinorCompactionTask implements CompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinorCompactionTask.class);
  private final SegmentManager manager;
  private final List<Segment> segments;

  public MinorCompactionTask(SegmentManager manager, List<Segment> segments) {
    this.manager = Assert.notNull(manager, "manager");
    this.segments = Assert.notNull(segments, "segments");
  }

  @Override
  public void run() {
    cleanSegments();
  }

  /**
   * Cleans all cleanable segments.
   */
  private void cleanSegments() {
    // Get the first segment which contains the first index being cleaned. The clean segment will be written
    // as a newer version of the earliest segment being rewritten.
    Segment firstSegment = segments.iterator().next();

    // Create a clean segment with a newer version to which to rewrite the segment entries.
    Segment cleanSegment = manager.createSegment(SegmentDescriptor.builder()
      .withId(firstSegment.descriptor().id())
      .withVersion(firstSegment.descriptor().version() + 1)
      .withIndex(firstSegment.descriptor().index())
      .withMaxEntrySize(segments.stream().mapToInt(s -> s.descriptor().maxEntrySize()).max().getAsInt())
      .withMaxSegmentSize(segments.stream().mapToLong(s -> s.descriptor().maxSegmentSize()).max().getAsLong())
      .withMaxEntries(segments.stream().mapToInt(s -> s.descriptor().maxEntries()).max().getAsInt())
      .build());

    // Clean the first entry from the segment to ensure the clean segment is not empty when inserted into the segment manager.
    cleanEntry(firstSegment.firstIndex(), firstSegment, cleanSegment);

    // Insert the new clean segment into the segment manager.
    manager.insertSegment(cleanSegment);

    // Iterate through all segments being compacted and write entries to a single clean segment.
    for (Segment segment : segments) {
      cleanSegment(segment, cleanSegment);
    }

    // Update the clean segment descriptor and lock the segment.
    cleanSegment.descriptor().update(System.currentTimeMillis());
    cleanSegment.descriptor().lock();

    // Delete the old segments.
    for (Segment segment : segments) {
      segment.delete();
    }
  }

  /**
   * Cleans the given segment.
   *
   * @param segment The segment to clean.
   * @param cleanSegment The segment to which to write the cleaned segment.
   */
  private void cleanSegment(Segment segment, Segment cleanSegment) {
    while (!segment.isEmpty()) {
      // Logic inside the Segment ensures that the firstIndex() is always reflective of the first index that
      // has not been compact()ed from the segment.
      long index = segment.firstIndex();

      // Clean the entry at the current index from the segment.
      cleanEntry(index, segment, cleanSegment);

      // Once the entry has been cleaned, update the segment manager to point to the correct segment.
      manager.moveSegment(index, segment);
    }
  }

  /**
   * Cleans the entry at the given index.
   *
   * @param index The index at which to clean the entry.
   * @param segment The segment to clean.
   * @param cleanSegment The segment to which to write the cleaned segment.
   */
  private void cleanEntry(long index, Segment segment, Segment cleanSegment) {
    try (Entry entry = segment.get(index)) {
      // If an entry was found, only remove the entry from the segment if it's not a tombstone that has been cleaned.
      if (entry != null) {
        // If the entry has been cleaned, determine whether it's a tombstone.
        if (segment.isClean(index)) {
          // If the entry is a tombstone, append the entry to the segment and clean it.
          // if the entry is not a tombstone, clean the entry.
          if (!entry.isTombstone()) {
            cleanSegment.skip(1);
            LOGGER.debug("Cleaned entry {} from segment {}", index, segment.descriptor().id());
          } else {
            cleanSegment.append(entry);
            cleanSegment.clean(index);
          }
        }
        // If the entry hasn't been cleaned, simply transfer it to the new segment.
        else {
          cleanSegment.append(entry);
        }
      }
      // If the entry has already been compacted, skip the index in the segment.
      else {
        cleanSegment.skip(1);
      }
    }

    // Compact the segment to move the segment's firstIndex() to index + 1.
    segment.compact(index + 1);
  }

  @Override
  public String toString() {
    return String.format("%s%s", getClass().getSimpleName(), segments);
  }

}
