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

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.FileBuffer;
import io.atomix.catalyst.buffer.MappedBuffer;
import io.atomix.catalyst.buffer.SlicedBuffer;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.function.Predicate;

/**
 * Stores a set of sequential entries in a single file or memory {@link Buffer}.
 * <p>
 * Segments are individual file or memory based groups of sequential entries. Each segment has a fixed capacity
 * in terms of either number of entries or size in bytes.
 * <p>
 * The {@link SegmentDescriptor} describes the metadata for the segment, including the starting {@code index} of
 * the segment and various configuration options. The descriptor is persisted in 48 bytes at the head of the segment.
 * <p>
 * Internally, each segment maintains an in-memory index of entries. The index stores the offset and position of
 * each entry within the segment's internal {@link io.atomix.catalyst.buffer.Buffer}. For entries that are appended
 * to the log sequentially, the index has an O(1) lookup time. For instances where entries in a segment have been
 * skipped (due to log compaction), the lookup time is O(log n) due to binary search. However, due to the nature of
 * the Raft consensus algorithm, readers should typically benefit from O(1) lookups.
 * <p>
 * When a segment is constructed, the segment will attempt to rebuild its index from the underlying segment
 * {@link Buffer}. This is done by reading a 16-bit unsigned length and 32-bit offset for each entry. Once the
 * segment has been built, new entries will be {@link #append(Entry) appended} at the end of the segment.
 * <p>
 * Additionally, segments are responsible for keeping track of entries that have been {@link #clean(long) cleaned}.
 * Cleaned entries are tracked in an internal {@link io.atomix.catalyst.buffer.util.BitArray} with a size equal
 * to the segment's entry {@link #count()}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Segment implements AutoCloseable {
  private final SegmentDescriptor descriptor;
  private final Serializer serializer;
  private final Buffer buffer;
  private final OffsetIndex offsetIndex;
  private final OffsetCleaner cleaner;
  private final SegmentManager manager;
  private long skip = 0;
  private boolean open = true;

  /**
   * @throws NullPointerException if any argument is null
   */
  Segment(Buffer buffer, SegmentDescriptor descriptor, OffsetIndex offsetIndex, OffsetCleaner cleaner, Serializer serializer, SegmentManager manager) {
    this.serializer = Assert.notNull(serializer, "serializer");
    this.buffer = Assert.notNull(buffer, "buffer");
    this.descriptor = Assert.notNull(descriptor, "descriptor");
    this.offsetIndex = Assert.notNull(offsetIndex, "offsetIndex");
    this.cleaner = Assert.notNull(cleaner, "cleaner");
    this.manager = Assert.notNull(manager, "manager");

    // Rebuild the index from the segment data.
    long position = buffer.mark().position();
    int length = buffer.readUnsignedShort();
    while (length != 0) {
      long offset = buffer.readLong();
      offsetIndex.index(offset, position);
      position = buffer.skip(length).position();
      length = buffer.mark().readUnsignedShort();
    }
    buffer.reset();
  }

  /**
   * Returns the {@link SegmentDescriptor} for the segment.
   * <p>
   * The segment descriptor is stored in {@link SegmentDescriptor#BYTES} bytes at the head of the segment. The descriptor
   * defines essential information about the segment, including its position in the complete {@link Log} and its {@code index}.
   *
   * @return The segment descriptor stored at the head of the segment.
   */
  public SegmentDescriptor descriptor() {
    return descriptor;
  }

  /**
   * Returns a boolean value indicating whether the segment is open.
   *
   * @return Indicates whether the segment is open.
   */
  public boolean isOpen() {
    return open;
  }

  /**
   * Returns a boolean value indicating whether the segment is empty.
   * <p>
   * The segment is considered empty if no entries have been written to the segment and no indexes in the
   * segment have been {@link #skip(long) skipped}.
   *
   * @return Indicates whether the segment is empty.
   */
  public boolean isEmpty() {
    return offsetIndex.size() > 0 ? offsetIndex.lastOffset() + 1 + skip == 0 : skip == 0;
  }

  /**
   * Returns a boolean value indicating whether the segment has been compacted.
   * <p>
   * The segment is considered compacted if its {@link SegmentDescriptor#version()} is greater than {@code 1}.
   *
   * @return Indicates whether the segment has been compacted.
   */
  public boolean isCompacted() {
    return descriptor.version() > 1;
  }

  /**
   * Returns a boolean value indicating whether the segment is full.
   * <p>
   * The segment is considered full if one of the following conditions is met:
   * <ul>
   *   <li>{@link #size()} is greater than or equal to {@link SegmentDescriptor#maxSegmentSize()}</li>
   *   <li>{@link #count()} is greater than or equal to {@link SegmentDescriptor#maxEntries()}</li>
   * </ul>
   *
   * @return Indicates whether the segment is full.
   */
  public boolean isFull() {
    return size() >= descriptor.maxSegmentSize()
      || offsetIndex.size() >= descriptor.maxEntries();
  }

  /**
   * Returns the total size of the segment in bytes.
   *
   * @return The size of the segment in bytes.
   */
  public long size() {
    return buffer.offset() + buffer.position();
  }

  /**
   * Returns the current range of the segment.
   * <p>
   * The length includes entries that may have been {@link #skip(long) skipped} at the end of the segment.
   *
   * @return The current range of the segment.
   */
  public long length() {
    return !isEmpty() ? offsetIndex.lastOffset() + 1 + skip : 0;
  }

  /**
   * Returns the count of all entries in the segment.
   * <p>
   * The count includes only entries that are physically present in the segment. Entries that have been compacted
   * out of the segment are not counted towards the count, nor are {@link #skip(long) skipped} entries.
   *
   * @return The count of all entries in the segment.
   */
  public int count() {
    return offsetIndex.size();
  }

  /**
   * Returns the base index of the segment.
   * <p>
   * The base index is equivalent to the segment's {@link #firstIndex()} if the segment is not {@link #isEmpty() emtpy}.
   *
   * @return The base index of the segment.
   */
  long index() {
    return descriptor.index();
  }

  /**
   * Returns the index of the first entry in the segment.
   * <p>
   * If the segment is empty, {@code 0} will be returned regardless of the segment's base index.
   *
   * @return The index of the first entry in the segment or {@code 0} if the segment is empty.
   * @throws IllegalStateException if the segment is not open
   */
  public long firstIndex() {
    assertSegmentOpen();
    return !isEmpty() ? descriptor.index() : 0;
  }

  /**
   * Returns the index of the last entry in the segment.
   *
   * @return The index of the last entry in the segment or {@code 0} if the segment is empty.
   * @throws IllegalStateException if the segment is not open
   */
  public long lastIndex() {
    assertSegmentOpen();
    return !isEmpty() ? offsetIndex.lastOffset() + descriptor.index() + skip : descriptor.index() - 1;
  }

  /**
   * Returns the next index in the segment.
   *
   * @return The next index in the segment.
   */
  public long nextIndex() {
    return !isEmpty() ? lastIndex() + 1 : descriptor.index() + skip;
  }

  /**
   * Returns the offset of the given index within the segment.
   * <p>
   * The offset reflects the zero-based offset of the given {@code index} in the segment when missing/compacted
   * entries are taken into account. For instance, if a segment contains entries at indexes {@code {1, 3}}, the
   * {@code offset} of index {@code 1} will be {@code 0} and index {@code 3} will be {@code 1}.
   *
   * @param index The index to check.
   * @return The offset of the given index.
   */
  public long offset(long index) {
    return offsetIndex.find(relativeOffset(index));
  }

  /**
   * Returns the offset for the given index.
   */
  private long relativeOffset(long index) {
    return index - descriptor.index();
  }

  /**
   * Checks the range of the given index.
   *
   * @throws IndexOutOfBoundsException if the {@code index} is invalid for the segment
   */
  private void checkRange(long index) {
    Assert.indexNot(isEmpty(), "segment is empty");
    Assert.indexNot(index < firstIndex(), index + " is less than the first index in the segment");
    Assert.indexNot(index > lastIndex(), index + " is greater than the last index in the segment");
  }

  /**
   * Commits an entry to the segment.
   *
   * @throws NullPointerException if {@code entry} is null
   * @throws IllegalStateException if the segment is full
   * @throws IndexOutOfBoundsException if the {@code entry} index does not match the next index
   */
  public long append(Entry entry) {
    Assert.notNull(entry, "entry");
    Assert.stateNot(isFull(), "segment is full");
    long index = nextIndex();
    Assert.index(index == entry.getIndex(), "inconsistent index: %s", entry.getIndex());

    // Calculate the offset of the entry.
    long offset = relativeOffset(index);

    // Mark the starting position of the record and record the starting position of the new entry.
    long position = buffer.mark().position();

    // Serialize the object into the segment buffer.
    serializer.writeObject(entry, buffer.skip(Short.BYTES + Long.BYTES));

    // Calculate the length of the serialized bytes based on the resulting buffer position and the starting position.
    int length = (int) (buffer.position() - (position + Short.BYTES + Long.BYTES));

    // Set the entry size.
    entry.setSize(length);

    // Write the length of the entry for indexing.
    buffer.reset().writeUnsignedShort(length).writeLong(offset).skip(length);

    // Index the offset, position, and length.
    offsetIndex.index(offset, position);

    // Reset skip to zero since we wrote a new entry.
    skip = 0;

    return index;
  }

  /**
   * Reads the entry at the given index.
   *
   * @param index The index from which to read the entry.
   * @return The entry at the given index.
   * @throws IllegalStateException if the segment is not open or {@code index} is inconsistent with the entry
   */
  public synchronized <T extends Entry> T get(long index) {
    assertSegmentOpen();
    checkRange(index);

    // Get the offset of the index within this segment.
    long offset = relativeOffset(index);

    // Get the start position of the entry from the memory index.
    long position = offsetIndex.position(offset);

    // If the index contained the entry, read the entry from the buffer.
    if (position != -1) {

      // Read the length of the entry.
      int length = buffer.readUnsignedShort(position);

      // Verify that the entry at the given offset matches.
      long entryOffset = buffer.readLong(position + Short.BYTES);
      Assert.state(entryOffset == offset, "inconsistent index: %s", index);

      // Read the entry buffer and deserialize the entry.
      try (Buffer value = buffer.slice(position + Short.BYTES + Long.BYTES, length)) {
        T entry = serializer.readObject(value);
        entry.setIndex(index).setSize(length);
        return entry;
      }
    }
    return null;
  }

  /**
   * Returns a boolean value indicating whether the given index is within the range of the segment.
   *
   * @param index The index to check.
   * @return Indicates whether the given index is within the range of the segment.
   * @throws IllegalStateException if the segment is not open
   */
  boolean validIndex(long index) {
    assertSegmentOpen();
    return !isEmpty() && index >= firstIndex() && index <= lastIndex();
  }

  /**
   * Returns a boolean value indicating whether the entry at the given index is active.
   *
   * @param index The index to check.
   * @return Indicates whether the entry at the given index is active.
   * @throws IllegalStateException if the segment is not open
   */
  public boolean contains(long index) {
    assertSegmentOpen();

    if (!validIndex(index))
      return false;

    // Check the memory index first for performance reasons.
    long offset = relativeOffset(index);
    return offsetIndex.contains(offset);
  }

  /**
   * Cleans an entry from the segment.
   *
   * @param index The index of the entry to clean.
   * @return Indicates whether the entry was newly cleaned from the segment.
   * @throws IllegalStateException if the segment is not open
   */
  public boolean clean(long index) {
    assertSegmentOpen();
    long offset = offsetIndex.find(relativeOffset(index));
    return offset != -1 && cleaner.clean(offset);
  }

  /**
   * Returns a boolean value indicating whether the given index was cleaned from the segment.
   *
   * @param index The index of the entry to check.
   * @return Indicates whether the given entry was cleaned from the segment.
   * @throws IllegalStateException if the segment is not open
   */
  public boolean isClean(long index) {
    assertSegmentOpen();
    return cleaner.isClean(offsetIndex.find(relativeOffset(index)));
  }

  /**
   * Returns the number of entries in the segment that have been cleaned.
   *
   * @return The number of entries in the segment that have been cleaned.
   * @throws IllegalStateException if the segment is not open
   */
  public long cleanCount() {
    assertSegmentOpen();
    return cleaner.count();
  }

  /**
   * Returns a predicate for cleaned offsets in the segment.
   *
   * @return A predicate for cleaned offsets in the segment.
   */
  public Predicate<Long> cleanPredicate() {
    return new OffsetCleaner(cleaner.bits().copy());
  }

  /**
   * Skips a number of entries in the segment.
   *
   * @param entries The number of entries to skip.
   * @return The segment.
   * @throws IllegalStateException if the segment is not open
   */
  public Segment skip(long entries) {
    assertSegmentOpen();
    this.skip += entries;
    return this;
  }

  /**
   * Truncates entries after the given index.
   *
   * @param index The index after which to remove entries.
   * @return The segment.
   * @throws IllegalStateException if the segment is not open
   */
  public Segment truncate(long index) {
    assertSegmentOpen();
    Assert.index(index >= manager.commitIndex(), "cannot truncate committed index");

    long offset = relativeOffset(index);
    long lastOffset = offsetIndex.lastOffset();

    long diff = Math.abs(lastOffset - offset);
    skip = Math.max(skip - diff, 0);

    if (offset < lastOffset) {
      long position = offsetIndex.truncate(offset);
      buffer.position(position)
        .zero(position)
        .flush();
    }
    return this;
  }

  /**
   * Flushes the segment buffers to disk.
   *
   * @return The segment.
   */
  public Segment flush() {
    buffer.flush();
    offsetIndex.flush();
    return this;
  }

  @Override
  public void close() {
    buffer.close();
    offsetIndex.close();
    descriptor.close();
    open = false;
  }

  /**
   * Deletes the segment.
   */
  public void delete() {
    Buffer buffer = this.buffer instanceof SlicedBuffer ? ((SlicedBuffer) this.buffer).root() : this.buffer;
    if (buffer instanceof FileBuffer) {
      ((FileBuffer) buffer).delete();
    } else if (buffer instanceof MappedBuffer) {
      ((MappedBuffer) buffer).delete();
    }

    offsetIndex.delete();
  }

  @Override
  public String toString() {
    return String.format("Segment[id=%d, version=%d, index=%d, length=%d]", descriptor.id(), descriptor.version(), firstIndex(), length());
  }

  private void assertSegmentOpen() {
    Assert.state(isOpen(), "segment not open");
  }
}
