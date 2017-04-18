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
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.storage.Segment;
import io.atomix.copycat.server.storage.SegmentDescriptor;
import io.atomix.copycat.server.storage.SegmentManager;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.server.storage.util.OffsetPredicate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Removes tombstones from the log and combines {@link Segment}s to reclaim disk space.
 * <p>
 * Major compaction is a more heavyweight compaction task which is responsible both for removing <em>tombstone</em>
 * {@link Entry entries} from the log and combining groups of neighboring log {@link Segment}s together.
 * <p>
 * <b>Combining segments</b>
 * <p>
 * As entries are written to the log and the log rolls over to new segments, entries are compacted out of individual
 * segments by {@link MinorCompactionTask}s. However, the minor compaction process only rewrites individual segments
 * and doesn't combine them. This will result in an ever growing number of open file pointers. During major compaction,
 * the major compaction task rewrites groups of segments provided by the {@link MajorCompactionManager}. For each group
 * of segments, a single compact segment will be created with the same {@code version} and starting {@code index} as
 * the first segment in the group. All entries from all segments in the group that haven't been
 * {@link io.atomix.copycat.server.storage.Log#release(long) released} will then be written to the new compact segment.
 * Once the rewrite is complete, the compact segment will be locked and the set of old segments deleted.
 * <p>
 * <b>Removing tombstones</b>
 * <p>
 * Tombstones are {@link Entry entries} in the log which amount to state changes that <em>remove</em> state. That is,
 * tombstones are an indicator that some set of prior entries no longer contribute to the state of the system. Thus,
 * it is critical that tombstones remain in the log as long as any prior related entries do. If a tombstone is removed
 * from the log before its prior related entries, rebuilding state from the log will result in inconsistencies.
 * <p>
 * A significant objective of the major compaction task is to remove tombstones from the log in a manor that ensures
 * failures before, during, or after the compaction task will not result in inconsistencies when state is rebuilt from
 * the log. In order to ensure tombstones are removed only <em>after</em> any prior related entries, the major compaction
 * task simply compacts segments in sequential order from the {@link Segment#firstIndex()} of the first segment to the
 * {@link Segment#lastIndex()} of the last segment. This ensures that if a failure occurs during the compaction process,
 * only entries earlier in the log will have been removed, and potential tombstones which erase the state of those entries
 * will remain.
 * <p>
 * Nevertheless, there are some significant potential race conditions that must be considered in the implementation of
 * major compaction. The major compaction task assumes that state machines will always release <em>related</em> entries
 * in monotonically increasing order. That is, if a state machines receives a {@link io.atomix.copycat.server.Commit}
 * {@code remove 1} that deletes the state of a prior {@code Commit} {@code set 1}, the state machine will call
 * {@link Commit#close()} on the {@code set 1} commit before releasing the {@code remove 1} commit. But even if applications
 * release entries from the log in monotonic order, and the major compaction task compacts segments in sequential order,
 * inconsistencies can still arise. Consider the following history:
 * <ul>
 *   <li>{@code set 1} is at index {@code 1} in segment {@code 1}</li>
 *   <li>{@code remove 1} is at index {@code 12345} in segment {@code 8}</li>
 *   <li>The major compaction task rewrites segment {@code 1}</li>
 *   <li>The application releases {@code set 1} at index {@code 1} in the <em>rewritten</em> version of segment {@code 1}</li>
 *   <li>The application releases {@code remove 1} at index {@code 12345} in segment {@code 8}, which the compaction task
 *   has yet to compact</li>
 *   <li>The compaction task compacts segments {@code 2} through {@code 8}, removing tombstone entry {@code 12345} during
 *   the process</li>
 * </ul>
 * <p>
 * In the scenario above, the resulting log contains {@code set 1} but not {@code remove 1}. If we replayed those entries
 * as {@link Commit}s to the log, it would result in an inconsistent state. Worse yet, not only is this server's state
 * incorrect, but it will be inconsistent with other servers which are likely to have correctly removed both entry
 * {@code 1} and entry {@code 12345} during major compaction.
 * <p>
 * In order to prevent such a scenario from occurring, the major compaction task takes an immutable snapshot of the
 * state of offsets underlying all the segments to be compacted prior to rewriting any entries. This ensures that any
 * entries released after the start of rewriting segments will not be considered for compaction during the execution
 * of this task.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class MajorCompactionTask implements CompactionTask {
  private static final Logger LOGGER = LoggerFactory.getLogger(MajorCompactionTask.class);
  private final SegmentManager manager;
  private final List<List<Segment>> groups;
  private List<List<OffsetPredicate>> predicates;
  private final long snapshotIndex;
  private final long compactIndex;
  private final Compaction.Mode defaultCompactionMode;

  MajorCompactionTask(SegmentManager manager, List<List<Segment>> groups, long snapshotIndex, long compactIndex, Compaction.Mode defaultCompactionMode) {
    this.manager = Assert.notNull(manager, "manager");
    this.groups = Assert.notNull(groups, "segments");
    this.snapshotIndex = snapshotIndex;
    this.compactIndex = compactIndex;
    this.defaultCompactionMode = Assert.notNull(defaultCompactionMode, "defaultCompactionMode");
  }

  @Override
  public void run() {
    copyPredicates();
    compactGroups();
  }

  /**
   * Creates a copy of offset predicates prior to compacting segments to prevent race conditions.
   */
  private void copyPredicates() {
    predicates = new ArrayList<>(groups.size());
    for (List<Segment> group : groups) {
      List<OffsetPredicate> groupPredicates = new ArrayList<>(group.size());
      for (Segment segment : group) {
        groupPredicates.add(segment.offsetPredicate().copy());
      }
      predicates.add(groupPredicates);
    }
  }

  /**
   * Compacts all compactable segments.
   */
  private void compactGroups() {
    for (int i = 0; i < groups.size(); i++) {
      List<Segment> group = groups.get(i);
      List<OffsetPredicate> groupPredicates = predicates.get(i);
      Segment segment = compactGroup(group, groupPredicates);
      mergeReleased(group, groupPredicates, segment);
      deleteGroup(group);
    }
  }

  /**
   * Compacts a group.
   */
  private Segment compactGroup(List<Segment> segments, List<OffsetPredicate> predicates) {
    // Get the first segment which contains the first index being compacted. The compact segment will be written
    // as a newer version of the earliest segment being rewritten.
    Segment firstSegment = segments.iterator().next();

    // Create a compacted segment with a newer version to which to rewrite the segment entries.
    Segment compactSegment = manager.createSegment(SegmentDescriptor.builder()
      .withId(firstSegment.descriptor().id())
      .withVersion(firstSegment.descriptor().version() + 1)
      .withIndex(firstSegment.descriptor().index())
      .withMaxSegmentSize(Math.max(segments.stream().mapToLong(s -> s.descriptor().maxSegmentSize()).max().getAsLong(), manager.storage().maxSegmentSize()))
      .withMaxEntries(Math.max(segments.stream().mapToInt(s -> s.descriptor().maxEntries()).max().getAsInt(), manager.storage().maxEntriesPerSegment()))
      .build());

    compactGroup(segments, predicates, compactSegment);

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
  private void compactGroup(List<Segment> segments, List<OffsetPredicate> predicates, Segment compactSegment) {
    // Iterate through all segments being compacted and write entries to a single compact segment.
    for (int i = 0; i < segments.size(); i++) {
      compactSegment(segments.get(i), predicates.get(i), compactSegment);
    }
  }

  /**
   * Compacts the given segment.
   *
   * @param segment The segment to compact.
   * @param compactSegment The segment to which to write the compacted segment.
   */
  private void compactSegment(Segment segment, OffsetPredicate predicate, Segment compactSegment) {
    for (long i = segment.firstIndex(); i <= segment.lastIndex(); i++) {
      checkEntry(i, segment, predicate, compactSegment);
    }
  }

  /**
   * Compacts the entry at the given index.
   *
   * @param index The index at which to compact the entry.
   * @param segment The segment to compact.
   * @param compactSegment The segment to which to write the uncompacted segment.
   */
  private void checkEntry(long index, Segment segment, OffsetPredicate predicate, Segment compactSegment) {
    try (Entry entry = segment.get(index)) {
      // If an entry was found, remove the entry from the segment.
      if (entry != null) {
        checkEntry(index, entry, segment, predicate, compactSegment);
      } else {
        compactSegment.skip(1);
      }
    }
  }

  /**
   * Compacts a command entry from a segment.
   */
  private void checkEntry(long index, Entry entry, Segment segment, OffsetPredicate predicate, Segment compactSegment) {
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
        if (index <= snapshotIndex && !isLive(index, segment, predicate)) {
          compactEntry(index, segment, compactSegment);
        } else {
          transferEntry(entry, compactSegment);
        }
        break;
      // RELEASE and QUORUM entries are compacted if the entry has been released from the segment.
      case RELEASE:
      case QUORUM:
        if (!isLive(index, segment, predicate)) {
          compactEntry(index, segment, compactSegment);
        } else {
          transferEntry(entry, compactSegment);
        }
        break;
      // FULL entries are compacted if the major compact index is greater than the entry index and
      // the entry has been released.
      // SEQUENTIAL, EXPIRING, and TOMBSTONE entries are compacted if the major compact index is greater than the
      // entry index and the entry has been released.
      case FULL:
      case SEQUENTIAL:
      case EXPIRING:
      case TOMBSTONE:
        if (index <= compactIndex && !isLive(index, segment, predicate)) {
          compactEntry(index, segment, compactSegment);
        } else {
          transferEntry(entry, compactSegment);
        }
        break;
      // UNKNOWN entries are compacted if the index is less than both the snapshot and major
      // compaction indexes and the entry has been released.
      case UNKNOWN:
        if (index <= snapshotIndex && index <= compactIndex && !isLive(index, segment, predicate)) {
          compactEntry(index, segment, compactSegment);
        } else {
          transferEntry(entry, compactSegment);
        }
        break;
      default:
        break;
    }
  }

  /**
   * Compacts an entry from the given segment.
   */
  private void compactEntry(long index, Segment segment, Segment compactSegment) {
    compactSegment.skip(1);
    LOGGER.trace("Compacted entry {} from segment {}", index, segment.descriptor().id());
  }

  /**
   * Transfers an entry to the given segment.
   */
  private void transferEntry(Entry entry, Segment compactSegment) {
    compactSegment.append(entry);
  }

  /**
   * Returns a boolean value indicating whether the given index is release.
   */
  private boolean isLive(long index, Segment segment, OffsetPredicate predicate) {
    long offset = segment.offset(index);
    return offset != -1 && predicate.test(offset);
  }

  /**
   * Updates the new compact segment with entries that were released during compaction.
   */
  private void mergeReleased(List<Segment> segments, List<OffsetPredicate> predicates, Segment compactSegment) {
    for (int i = 0; i < segments.size(); i++) {
      mergeReleasedEntries(segments.get(i), predicates.get(i), compactSegment);
    }
  }

  /**
   * Updates the new compact segment with entries that were released in the given segment during compaction.
   */
  private void mergeReleasedEntries(Segment segment, OffsetPredicate predicate, Segment compactSegment) {
    for (long i = segment.firstIndex(); i <= segment.lastIndex(); i++) {
      long offset = segment.offset(i);
      if (offset != -1 && !predicate.test(offset)) {
        compactSegment.release(i);
      }
    }
  }

  /**
   * Completes compaction by deleting old segments.
   */
  private void deleteGroup(List<Segment> group) {
    // Delete the old segments.
    for (Segment oldSegment : group) {
      oldSegment.close();
      oldSegment.delete();
    }
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
