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
 * that do not have to be removed sequentially from the log.
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
  private final long snapshotIndex;
  private final long compactIndex;

  MinorCompactionTask(SegmentManager manager, Segment segment, long snapshotIndex, long compactIndex) {
    this.manager = Assert.notNull(manager, "manager");
    this.segment = Assert.notNull(segment, "segment");
    this.snapshotIndex = snapshotIndex;
    this.compactIndex = compactIndex;
  }

  @Override
  public void run() {
    compactSegments();
  }

  /**
   * Compacts all cleanable segments.
   */
  private void compactSegments() {
    // Create a compact segment with a newer version to which to rewrite the segment entries.
    Segment compactSegment = manager.createSegment(SegmentDescriptor.builder()
      .withId(segment.descriptor().id())
      .withVersion(segment.descriptor().version() + 1)
      .withIndex(segment.descriptor().index())
      .withMaxSegmentSize(segment.descriptor().maxSegmentSize())
      .withMaxEntries(segment.descriptor().maxEntries())
      .build());

    compactEntries(segment, compactSegment);

    // Replace the old segment with the compact segment.
    manager.replaceSegments(Collections.singletonList(segment), compactSegment);

    // Delete the old segment.
    segment.close();
    segment.delete();
  }

  /**
   * Compacts entries from the given segment, rewriting them to the compact segment.
   *
   * @param segment The segment to compact.
   * @param compactSegment The compact segment.
   */
  private void compactEntries(Segment segment, Segment compactSegment) {
    for (long i = segment.firstIndex(); i <= segment.lastIndex(); i++) {
      compactEntry(i, segment, compactSegment);
    }
  }

  /**
   * Compacts the entry at the given index.
   *
   * @param index The index at which to compact the entry.
   * @param segment The segment to compact.
   * @param cleanSegment The segment to which to write the compacted segment.
   */
  private void compactEntry(long index, Segment segment, Segment cleanSegment) {
    try (Entry entry = segment.get(index)) {
      // If an entry was found, only remove the entry from the segment if it's not a tombstone that has been cleaned.
      if (entry != null) {
        compactEntry(index, entry, segment, cleanSegment);
      } else {
        cleanSegment.skip(1);
      }
    }
  }


  /**
   * Cleans a command entry from a segment.
   */
  private void compactEntry(long index, Entry entry, Segment segment, Segment compactSegment) {
    // According to the entry's compaction mode, either append the entry to the compact segment
    // or skip the entry in the compact segment (removing it from the resulting segment).
    switch (entry.getCompactionMode()) {
      // Snapshot entries are compacted if a snapshot has been taken at an index greater than the
      // entry's index.
      case SNAPSHOT:
        if (index <= snapshotIndex) {
          compactSegment.skip(1);
          LOGGER.debug("Compacted entry {} from segment {}", index, segment.descriptor().id());
        } else {
          compactSegment.append(entry);
        }
        break;
      // Quorum committed entries are always compacted since the requirements for compaction of the
      // segment are that all entries have been stored on a majority of servers.
      case QUORUM_COMMIT:
        compactSegment.skip(1);
        LOGGER.debug("Compacted entry {} from segment {}", index, segment.descriptor().id());
        break;
      // Quorum cleaned entries are compacted if the entry has been marked clean in the segment.
      case QUORUM_CLEAN:
        if (segment.isClean(index)) {
          compactSegment.skip(1);
          LOGGER.debug("Compacted entry {} from segment {}", index, segment.descriptor().id());
        } else {
          compactSegment.append(entry);
        }
        break;
      // Full committed entries are compacted once the major compact index is greater than the entry index.
      case FULL_COMMIT:
        if (index <= compactIndex) {
          compactSegment.skip(1);
          LOGGER.debug("Compacted entry {} from segment {}", index, segment.descriptor().id());
        } else {
          compactSegment.append(entry);

          // If the entry was cleaned in the prior segment, mark it as cleaned in the compact segment.
          if (segment.isClean(index)) {
            compactSegment.clean(index);
          }
        }
        break;
      // Full clean entries are compacted once the major compact index is greater than the entry index
      // and the entry has been cleaned.
      case FULL_CLEAN:
        if (index <= compactIndex && segment.isClean(index)) {
          compactSegment.skip(1);
          LOGGER.debug("Compacted entry {} from segment {}", index, segment.descriptor().id());
        } else {
          compactSegment.append(entry);

          // If the entry was cleaned in the prior segment, mark it as cleaned in the compact segment.
          if (segment.isClean(index)) {
            compactSegment.clean(index);
          }
        }
        break;
      // Sequentially compacted entries can only be compacted during major compaction.
      case FULL_SEQUENTIAL_COMMIT:
      case FULL_SEQUENTIAL_CLEAN:
        compactSegment.append(entry);

        // If the entry was cleaned in the prior segment, mark it as cleaned in the compact segment.
        if (segment.isClean(index)) {
          compactSegment.clean(index);
        }
        break;
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
