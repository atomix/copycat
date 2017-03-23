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
 * limitations under the License.
 */
package io.atomix.copycat.server.storage;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.concurrent.CatalystThreadFactory;
import io.atomix.copycat.server.storage.compaction.Compaction;
import io.atomix.copycat.server.storage.compaction.Compactor;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.server.storage.entry.TypedEntryPool;
import io.atomix.copycat.server.storage.util.EntryBuffer;

import java.util.concurrent.Executors;

/**
 * Stores Raft log entries in a segmented log in memory or on disk.
 * <p>
 * The log is the primary vehicle for storing state changes and managing replication in Raft. The log is used to verify
 * consistency between members and manage cluster configurations, client sessions, state machine operations, and other
 * tasks.
 * <p>
 * State changes are written to the log as {@link Entry} objects. Each entry is associated with an {@code index} and
 * {@code term}. The {@code index} is a 1-based entry index from the start of the log. The {@code term} is used for
 * various consistency checks in the Raft algorithm. Raft guarantees that a {@link #commit(long) committed} entry at any
 * index {@code i} that has term {@code t} will also be present in the logs on all other servers in the cluster at the
 * same index {@code i} with term {@code t}. However, note that log compaction may break this contract. Considering log
 * compaction, it's more accurate to say that iff committed entry {@code i} is present in another server's log, that
 * entry has term {@code t} and the same value.
 * <p>
 * Entries are written to the log via the {@link #append(Entry)} method. When an entry is appended, it's written to the
 * next sequential index in the log after {@link #lastIndex()}. Entries can be created from a typed entry pool with the
 * {@link #create(Class)} method. <pre>
 *   {@code
 *   long index;
 *   try (CommandEntry entry = log.create(CommandEntry.class)) {
 *     entry.setTerm(2)
 *       .setSequence(5)
 *       .setCommand(new PutCommand());
 *     index = log.append(entry);
 *   }
 *   }
 * </pre>
 * <p>
 * {@link Entry entries} are appended to {@link Segment}s in the log. Segments are individual file or memory based
 * groups of sequential entries. Each segment has a fixed capacity in terms of either number of entries or size in
 * bytes. Once the capacity of a segment has been reached, the log rolls over to a new segment for the next entry that's
 * appended.
 * <p>
 * Internally, each segment maintains an in-memory index of entries. The index stores the offset and position of each
 * entry within the segment's internal {@link io.atomix.catalyst.buffer.Buffer}. For entries that are appended to the
 * log sequentially, the index has an O(1) lookup time. For instances where entries in a segment have been skipped (due
 * to log compaction), the lookup time is O(log n) due to binary search. However, due to the nature of the Raft
 * consensus algorithm, readers should typically benefit from O(1) lookups.
 * <p>
 * In order to prevent exhausting disk space, the log manages a set of background threads that periodically rewrite and
 * combine segments to free disk space. This is known as log compaction. As entries are committed to the log and applied
 * to the Raft state machine as {@link io.atomix.copycat.server.Commit} objects, state machines {@link #release(long)}
 * entries that no longer apply to the state machine state. Internally, each log {@link Segment} maintains a compact
 * {@link io.atomix.catalyst.buffer.util.BitArray} to track the liveness of entries. When an entry is released, the entry's
 * offset is set in the bit array for the associated segment. The bit array represents the state of entries waiting to
 * be compacted from the log.
 * <p>
 * As entries are written to the log, segments reach their capacity and the log rolls over into new segments. Once a
 * segment is full and all of its entries have been {@link #commit(long) committed}, indicating they cannot be removed,
 * the segment becomes eligible for compaction. Log compaction processes come in two forms:
 * {@link io.atomix.copycat.server.storage.compaction.Compaction#MINOR} and
 * {@link io.atomix.copycat.server.storage.compaction.Compaction#MAJOR}, which can be configured in the {@link Storage}
 * configuration. Minor and major compaction serve to remove normal entries and tombstones from the log respectively.
 * <p>
 * Minor compaction is the more frequent and lightweight process. Periodically, according to the configured
 * {@link Storage#minorCompactionInterval()}, a background thread will evaluate the log for minor compaction. The minor
 * compaction process iterates through segments and selects compactable segments based on the ratio of entries that have
 * been {@link #release(long) released}. Minor compaction is generational. The
 * {@link io.atomix.copycat.server.storage.compaction.MinorCompactionManager} is more likely to select segments that haven't
 * yet been compacted than ones that have. Once a set of segments have been compacted, for each segment a
 * {@link io.atomix.copycat.server.storage.compaction.MinorCompactionTask} rewrites the segment without released entries.
 * This rewriting results in a segment with missing entries, and Copycat's Raft implementation accounts for that. For
 * instance, a segment with entries {@code {1, 2, 3}} can become {@code {1, 3}} after being released, and any attempt to
 * {@link #get(long) read} entry {@code 2} will result in a {@code null} entry.
 * <p>
 * However, note that minor compaction only applies to non-tombstone entries. Tombstones are entries that represent the
 * removal of state from the system, and that requires a more careful and costly compaction process to ensure consistency
 * in the event of a failure. Consider a state machine with the following two commands in the log:
 * <ul>
 * <li>{@code put 1}</li>
 * <li>{@code remove 1}</li>
 * </ul>
 * If the first command is written to segment {@code 1}, and the second command is written to segment {@code 2},
 * compacting segment {@code 2} (minor compaction may compact segments in any order) without removing the first command
 * from segment {@code 1} would effectively result in the undoing of the {@code remove 1} command. If the {@code remove 1}
 * command is removed from the log before {@code put 1}, a restart and replay of the log will result in the application of
 * {@code put 1} to the state machine, but not {@code remove 1}, thus resulting in an inconsistent state machine state.
 * <p>
 * As entries are removed from the log during minor and major compaction, log segment files begin to shrink. Copycat
 * does not want to have a thousand file pointers open, so some mechanism is required to combine segments as disk space
 * is freed. To that end, as the major compaction process iterates through the set of committed segments and rewrites
 * live entries, it combines multiple segments up to the configured segment capacity. When a segment becomes full during
 * major compaction, the compaction process rolls over to a new segment and continues compaction. This results in a
 * significantly smaller number of files.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Log implements AutoCloseable {
  private final Storage storage;
  final SegmentManager segments;
  private final Compactor compactor;
  private final EntryBuffer entryBuffer;
  private final TypedEntryPool entryPool = new TypedEntryPool();
  private boolean open = true;

  /**
   * @throws NullPointerException if {@code name} or {@code storage} is null
   */
  protected Log(String name, Storage storage, Serializer serializer) {
    this.storage = Assert.notNull(storage, "storage");
    this.segments = new SegmentManager(name, storage, serializer);
    this.compactor = new Compactor(storage, segments, Executors.newScheduledThreadPool(storage.compactionThreads(), new CatalystThreadFactory("copycat-compactor-%d")));
    this.entryBuffer = new EntryBuffer(storage.entryBufferSize());
  }

  /**
   * Returns the log compactor.
   *
   * @return The log compactor.
   */
  public Compactor compactor() {
    return compactor;
  }

  /**
   * Returns the log entry serializer.
   *
   * @return The log entry serializer.
   */
  public Serializer serializer() {
    return segments.serializer();
  }

  /**
   * Returns a boolean value indicating whether the log is open.
   *
   * @return Indicates whether the log is open.
   */
  public boolean isOpen() {
    return open;
  }

  /**
   * Asserts that the log is open.
   *
   * @throws IllegalStateException if the log is not open
   */
  private void assertIsOpen() {
    Assert.state(isOpen(), "log is not open");
  }

  /**
   * Asserts that the index is a valid index.
   *
   * @throws IndexOutOfBoundsException if the {@code index} is out of bounds
   */
  private void assertValidIndex(long index) {
    Assert.index(validIndex(index), "invalid log index: %d", index);
  }

  /**
   * Returns a boolean value indicating whether the log is empty.
   *
   * @return Indicates whether the log is empty.
   * @throws IllegalStateException If the log is not open.
   */
  public boolean isEmpty() {
    assertIsOpen();
    return segments.firstSegment().isEmpty();
  }

  /**
   * Returns the total size of all {@link Segment segments} of the log on disk in bytes.
   *
   * @return The total size of all {@link Segment segments} of the log in bytes.
   * @throws IllegalStateException If the log is not open.
   */
  public long size() {
    assertIsOpen();
    return segments.segments().stream().mapToLong(Segment::size).sum();
  }

  /**
   * Returns the number of entries in the log.
   * <p>
   * The length is the total number of {@link Entry entries} represented by the log on disk. This includes entries
   * that have been compacted from the log. So, in that sense, the length represents the total range of indexes.
   *
   * @return The number of entries in the log.
   * @throws IllegalStateException If the log is not open.
   */
  public long length() {
    assertIsOpen();
    return segments.segments().stream().mapToLong(Segment::length).sum();
  }

  /**
   * Returns the log's current first index.
   * <p>
   * If no entries have been written to the log then the first index will be {@code 0}. If the log contains entries then
   * the first index will be {@code 1}.
   *
   * @return The index of the first entry in the log or {@code 0} if the log is empty.
   * @throws IllegalStateException If the log is not open.
   */
  public long firstIndex() {
    return !isEmpty() ? segments.firstSegment().descriptor().index() : 0;
  }

  /**
   * Returns the index of the last entry in the log.
   * <p>
   * If no entries have been written to the log then the last index will be {@code 0}.
   *
   * @return The index of the last entry in the log or {@code 0} if the log is empty.
   * @throws IllegalStateException If the log is not open.
   */
  public long lastIndex() {
    return !isEmpty() ? segments.lastSegment().lastIndex() : 0;
  }

  /**
   * Returns the next index in the log.
   *
   * @return The next index in the log.
   */
  public long nextIndex() {
    return lastIndex() + 1;
  }

  /**
   * Checks whether we need to roll over to a new segment.
   */
  private Segment currentSegment() {
    Segment segment = segments.currentSegment();
    if (segment.isFull()) {
      segments.currentSegment().flush();
      segment = segments.nextSegment();
    }
    return segment;
  }

  /**
   * Creates a new log entry.
   * <p>
   * Users should ensure that the returned {@link Entry} is closed once the write is complete. Closing the entry will
   * result in its contents being persisted to the log. Only a single {@link Entry} instance may be open via the this
   * method at any given time.
   *
   * @param type The entry type.
   * @return The log entry.
   * @throws IllegalStateException If the log is not open
   * @throws NullPointerException If the {@code type} is {@code null}
   */
  public <T extends Entry<T>> T create(Class<T> type) {
    Assert.notNull(type, "type");
    assertIsOpen();
    return entryPool.acquire(type, currentSegment().nextIndex());
  }

  /**
   * Appends an entry to the log.
   *
   * @param entry The entry to append.
   * @return The appended entry index.
   * @throws IllegalStateException If the log is not open
   * @throws NullPointerException If {@code entry} is {@code null}
   * @throws IndexOutOfBoundsException If the entry's index does not match the expected next log index.
   */
  public long append(Entry entry) {
    Assert.notNull(entry, "entry");
    assertIsOpen();

    // Append the entry to the appropriate segment.
    long index = currentSegment().append(entry);
    entryBuffer.append(entry);
    return index;
  }

  /**
   * Returns the term for the entry at the given index.
   * <p>
   * This method provides a more efficient means of reading an entry term without deserializing the entire entry.
   * Servers should use this method when performing consistency checks that don't require reading the full entry
   * object. Terms can typically be read in O(1) time with no disk access on segments that haven't been compacted.
   * <p>
   * If the given index is outside of the bounds of the log then a {@link IndexOutOfBoundsException} will be thrown. If
   * the entry at the given index has been compacted then the returned entry will be {@code null}.
   *
   * @param index The index for which to return the term.
   * @return The term for the entry at the given index.
   */
  public long term(long index) {
    assertIsOpen();
    assertValidIndex(index);

    Segment segment = segments.segment(index);
    Assert.index(segment != null, "invalid index: " + index);

    return segment.term(index);
  }

  /**
   * Gets an entry from the log at the given index.
   * <p>
   * If the given index is outside of the bounds of the log then a {@link IndexOutOfBoundsException} will be thrown. If
   * the entry at the given index has been compacted then the returned entry will be {@code null}.
   * <p>
   * Entries returned by this method are pooled and {@link io.atomix.catalyst.util.reference.ReferenceCounted reference counted}.
   * In order to ensure the entry is released back to the internal entry pool call {@link Entry#close()} or load the
   * entry in a try-with-resources statement. <pre>
   *   {@code
   *   try (RaftEntry entry = log.get(123)) {
   *     // Do some stuff...
   *   }
   *   }
   * </pre>
   *
   * @param index The index of the entry to get.
   * @return The entry at the given index or {@code null} if the entry doesn't exist.
   * @throws IllegalStateException If the log is not open.
   * @throws IndexOutOfBoundsException If the given index is not within the bounds of the log.
   */
  public <T extends Entry> T get(long index) {
    assertIsOpen();
    assertValidIndex(index);

    Segment segment = segments.segment(index);
    Assert.index(segment != null, "invalid index: " + index);

    // Get the entry from the segment. If the entry hasn't already been compacted from the segment,
    // it will be non-null.
    T entry = entryBuffer.get(index);
    if (entry == null) {
      entry = segment.get(index);
    }

    // For non-null entries, we determine whether the entry should be exposed to the Raft algorithm
    // based on the type of entry and whether it has been released.
    if (entry != null) {
      // The last entry in the log is always visible. This is necessary to ensure that candidates
      // can properly read the last entry term for the voting protocol.
      if (index == lastIndex()) {
        return entry;
      }

      Compaction.Mode mode = entry.getCompactionMode();
      if (mode == Compaction.Mode.DEFAULT) {
        mode = compactor.getDefaultCompactionMode();
      }

      // Return the entry according to the compaction mode.
      switch (mode) {
        // SNAPSHOT entries are returned if the snapshotIndex is less than the entry index.
        case SNAPSHOT:
          if (index > compactor.snapshotIndex()) {
            return entry;
          }
          break;
        // RELEASE and QUORUM entries are returned if the minorIndex is less than the entry index or the
        // entry is still live.
        case RELEASE:
        case QUORUM:
          if (index > compactor.minorIndex() || segment.isLive(index)) {
            return entry;
          }
          break;
        // FULL, SEQUENTIAL, EXPIRING, and TOMBSTONE entries are returned if the minorIndex or majorIndex is less than the
        // entry index or if the entry is still live.
        case FULL:
        case SEQUENTIAL:
        case EXPIRING:
        case TOMBSTONE:
          if (index > compactor.minorIndex() || index > compactor.majorIndex() || segment.isLive(index)) {
            return entry;
          }
          break;
        default:
          break;
      }
    }
    return null;
  }

  /**
   * Returns a boolean value indicating whether the given index is within the bounds of the log.
   * <p>
   * If the index is less than {@code 1} or greater than {@link Log#lastIndex()} then this method will return
   * {@code false}, otherwise {@code true}.
   *
   * @param index The index to check.
   * @return Indicates whether the given index is within the bounds of the log.
   * @throws IllegalStateException If the log is not open.
   */
  private boolean validIndex(long index) {
    long firstIndex = firstIndex();
    long lastIndex = lastIndex();
    return !isEmpty() && firstIndex <= index && index <= lastIndex;
  }

  /**
   * Returns a boolean value indicating whether the log contains a live entry at the given index.
   *
   * @param index The index to check.
   * @return Indicates whether the log contains a live entry at the given index.
   * @throws IllegalStateException If the log is not open.
   */
  public boolean contains(long index) {
    if (!validIndex(index))
      return false;

    Segment segment = segments.segment(index);
    return segment != null && segment.contains(index);
  }

  /**
   * Releases the entry at the given index.
   *
   * @param index The index of the entry to release.
   * @return The log.
   * @throws IllegalStateException If the log is not open.
   * @throws IndexOutOfBoundsException If the given index is not within the bounds of the log.
   */
  public Log release(long index) {
    assertIsOpen();
    assertValidIndex(index);

    Segment segment = segments.segment(index);
    Assert.index(segment != null, "invalid index: " + index);
    segment.release(index);
    return this;
  }

  /**
   * Commits entries up to the given index to the log.
   *
   * @param index The index up to which to commit entries.
   * @return The log.
   * @throws IllegalStateException If the log is not open.
   */
  public Log commit(long index) {
    assertIsOpen();
    if (index > 0) {
      assertValidIndex(index);
      segments.commitIndex(index);
      if (storage.flushOnCommit()) {
        segments.currentSegment().flush();
      }
    }
    return this;
  }

  /**
   * Skips the given number of entries.
   * <p>
   * This method essentially advances the log's {@link Log#lastIndex()} without writing any entries at the interim
   * indices. Note that calling {@code Loggable#truncate()} after {@code skip()} will result in the skipped entries
   * being partially or completely reverted.
   *
   * @param entries The number of entries to skip.
   * @return The log.
   * @throws IllegalStateException If the log is not open.
   * @throws IllegalArgumentException If the number of entries is less than {@code 1}
   * @throws IndexOutOfBoundsException If skipping the given number of entries places the index out of the bounds of the
   *           log.
   */
  public Log skip(long entries) {
    assertIsOpen();
    Segment segment = segments.currentSegment();
    segment.skip(entries);
    return this;
  }

  /**
   * Truncates the log.
   *
   * @return The truncated log.
   * @throws IllegalStateException If the log is not open.
   */
  public Log truncate() {
    return truncate(0);
  }

  /**
   * Truncates the log up to the given index.
   *
   * @param index The index at which to truncate the log.
   * @return The updated log.
   * @throws IllegalStateException If the log is not open.
   * @throws IndexOutOfBoundsException If the given index is not within the bounds of the log.
   */
  public Log truncate(long index) {
    assertIsOpen();
    if (index > 0)
      assertValidIndex(index);
    Assert.index(index >= segments.commitIndex(), "cannot truncate committed entries");

    if (lastIndex() == index)
      return this;

    for (Segment segment : segments.reverseSegments()) {
      if (segment.validIndex(index)) {
        segment.truncate(index);
        break;
      } else if (segment.index() > index) {
        segments.removeSegment(segment);
      }
    }
    entryBuffer.clear();
    return this;
  }

  /**
   * Flushes the log to disk.
   *
   * @throws IllegalStateException If the log is not open.
   */
  public void flush() {
    assertIsOpen();
    segments.currentSegment().flush();
  }

  /**
   * Closes the log.
   *
   * @throws IllegalStateException If the log is not open.
   */
  @Override
  public void close() {
    assertIsOpen();
    flush();
    compactor.close();
    segments.close();
    open = false;
  }

  /**
   * Returns a boolean value indicating whether the log is closed.
   *
   * @return Indicates whether the log is closed.
   */
  public boolean isClosed() {
    return !open;
  }

  @Override
  public String toString() {
    return String.format("%s[segments=%s]", getClass().getSimpleName(), segments);
  }

}
