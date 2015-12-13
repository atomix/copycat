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
  private final Compaction.Mode defaultCompactionMode;

  MinorCompactionTask(SegmentManager manager, Segment segment, long snapshotIndex, long compactIndex, Compaction.Mode defaultCompactionMode) {
    this.manager = Assert.notNull(manager, "manager");
    this.segment = Assert.notNull(segment, "segment");
    this.snapshotIndex = snapshotIndex;
    this.compactIndex = compactIndex;
    this.defaultCompactionMode = Assert.notNull(defaultCompactionMode, "defaultCompactionMode");
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
      checkEntry(i, segment, compactSegment);
    }
  }

  /**
   * Compacts the entry at the given index.
   *
   * @param index The index at which to compact the entry.
   * @param segment The segment to compact.
   * @param cleanSegment The segment to which to write the compacted segment.
   */
  private void checkEntry(long index, Segment segment, Segment cleanSegment) {
    try (Entry entry = segment.get(index)) {
      // If an entry was found, only remove the entry from the segment if it's not a tombstone that has been cleaned.
      if (entry != null) {
        checkEntry(index, entry, segment, cleanSegment);
      } else {
        cleanSegment.skip(1);
      }
    }
  }

  /**
   * Cleans a command entry from a segment.
   */
  private void checkEntry(long index, Entry entry, Segment segment, Segment compactSegment) {
    // Get the entry compaction mode. If the compaction mode is DEFAULT apply the default compaction
    // mode to the entry.
    Compaction.Mode mode = entry.getCompactionMode();
    if (mode == Compaction.Mode.DEFAULT) {
      mode = defaultCompactionMode;
    }

    // According to the entry's compaction mode, either append the entry to the compact segment
    // or skip the entry in the compact segment (removing it from the resulting segment).
    switch (mode) {
      // SNAPSHOT entries are compacted if a snapshot has been taken at an index greater than the
      // entry's index.
      case SNAPSHOT:
        if (index <= snapshotIndex && segment.isClean(index)) {
          compactEntry(index, segment, compactSegment);
        } else {
          transferEntry(index, entry, compactSegment);
        }
        break;
      // QUORUM entries are compacted if the entry has been marked clean in the segment.
      case QUORUM:
        if (segment.isClean(index)) {
          compactEntry(index, segment, compactSegment);
        } else {
          transferEntry(index, entry, compactSegment);
        }
        break;
      // FULL entries are compacted if the major compact index is greater than the entry index
      // and the entry has been cleaned.
      case FULL:
        if (index <= compactIndex && segment.isClean(index)) {
          compactEntry(index, segment, compactSegment);
        } else {
          transferEntry(index, entry, compactSegment);
        }
        break;
      // SEQUENTIAL entries can only be compacted during major compaction.
      // UNKNOWN entries can only be compacted during major compaction.
      case SEQUENTIAL:
      case UNKNOWN:
        transferEntry(index, entry, compactSegment);
        break;
    }
  }

  /**
   * Compacts an entry from the given segment.
   */
  private void compactEntry(long index, Segment segment, Segment compactSegment) {
    compactSegment.skip(1);
    LOGGER.debug("Compacted entry {} from segment {}", index, segment.descriptor().id());
  }

  /**
   * Transfers an entry to the given compact segment.
   */
  private void transferEntry(long index, Entry entry, Segment compactSegment) {
    compactSegment.append(entry);

    // If the entry was cleaned in the prior segment, mark it as cleaned in the compact segment.
    if (segment.isClean(index)) {
      compactSegment.clean(index);
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
