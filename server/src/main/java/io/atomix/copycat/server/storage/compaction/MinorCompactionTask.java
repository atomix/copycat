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

import java.util.Collections;

/**
 * Removes {@link io.atomix.copycat.server.storage.Log#clean(long) cleaned} entries from an individual
 * log {@link Segment} to reclaim disk space.
 * <p>
 * The minor compaction task is a lightweight process that rewrites an individual segment to remove entries for
 * which {@link Entry#isTombstone()} is {@code false}. Tombstones are entries that represent the deletion of
 * state from the application, and tombstones are not removed during minor compaction since the safety of removing
 * tombstones relies on expensive context. For more information on the implementation for removing tombstones,
 * see the {@link MajorCompactionTask}.
 * <p>
 * When a segment is rewritten by the minor compaction task, a new compact segment is created with the same starting
 * index as the segment being compacted and the next greatest version number. The version number allows the
 * {@link SegmentManager} to account for failures during log compaction when recovering the log from disk. If a failure
 * occurs during minor compaction, the segment manager will attempt to load the segment with the greatest version
 * for a given range of entries from disk. If the segment with the greatest version did not finish compaction, it
 * will be discarded and the old segment will be used. Once the minor compaction task is done rewriting a segment,
 * it will {@link SegmentDescriptor#lock()} the segment to indicate that the segment has completed compaction and
 * is safe to read, and the compacted segment will be made available to the {@link io.atomix.copycat.server.storage.Log}.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class MinorCompactionTask implements CompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(MinorCompactionTask.class);
  private final SegmentManager manager;
  private final Segment segment;

  MinorCompactionTask(SegmentManager manager, Segment segment) {
    this.manager = Assert.notNull(manager, "manager");
    this.segment = Assert.notNull(segment, "segment");
  }

  @Override
  public void run() {
    cleanSegments();
  }

  /**
   * Compacts all cleanable segments.
   */
  private void cleanSegments() {
    // Create a compact segment with a newer version to which to rewrite the segment entries.
    Segment compactSegment = manager.createSegment(SegmentDescriptor.builder()
      .withId(segment.descriptor().id())
      .withVersion(segment.descriptor().version() + 1)
      .withIndex(segment.descriptor().index())
      .withMaxSegmentSize(segment.descriptor().maxSegmentSize())
      .withMaxEntries(segment.descriptor().maxEntries())
      .build());

    cleanEntries(segment, compactSegment);

    // Replace the old segment with the compact segment.
    manager.replaceSegments(Collections.singletonList(segment), compactSegment);

    // Delete the old segment.
    segment.delete();
  }

  /**
   * Compacts entries from the given segment, rewriting them to the compact segment.
   *
   * @param segment The segment to compact.
   * @param compactSegment The compact segment.
   */
  private void cleanEntries(Segment segment, Segment compactSegment) {
    for (long i = segment.firstIndex(); i <= segment.lastIndex(); i++) {
      cleanEntry(i, segment, compactSegment);
    }
  }

  /**
   * Compacts the entry at the given index.
   *
   * @param index The index at which to compact the entry.
   * @param segment The segment to compact.
   * @param cleanSegment The segment to which to write the compacted segment.
   */
  private void cleanEntry(long index, Segment segment, Segment cleanSegment) {
    try (Entry entry = segment.get(index)) {
      // If an entry was found, only remove the entry from the segment if it's not a tombstone that has been cleaned.
      if (entry != null) {
        // If the entry has been cleaned, determine whether it's a tombstone.
        if (segment.isClean(index)) {
          // If the entry is not a tombstone, clean the entry.
          // If the entry is a tombstone, append the entry to the segment and clean it.
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
  }

  @Override
  public String toString() {
    return String.format("%s[segment=%s]", getClass().getSimpleName(), segment);
  }

}
