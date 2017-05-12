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

import java.util.zip.CRC32;
import java.util.zip.Checksum;

import io.atomix.catalyst.buffer.*;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.server.storage.index.OffsetIndex;
import io.atomix.copycat.server.storage.util.OffsetPredicate;
import io.atomix.copycat.server.storage.util.TermIndex;

import static io.atomix.catalyst.buffer.Bytes.BOOLEAN;
import static io.atomix.catalyst.buffer.Bytes.INTEGER;
import static io.atomix.catalyst.buffer.Bytes.LONG;

/**
 * Stores a sequence of entries with monotonically increasing indexes in a {@link Buffer}.
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
 * {@link Buffer}. This is done by reading a 32-bit length and 64-bit offset for each entry. Once the segment
 * has been built, new entries will be {@link #append(Entry) appended} at the end of the segment.
 * <p>
 * Additionally, segments are responsible for keeping track of entries that have been {@link #release(long) released}.
 * Entry liveness is tracked in an internal {@link io.atomix.catalyst.buffer.util.BitArray} with a size equal
 * to the segment's entry {@link #count()}.
 * <p>
 * An entry in the log is written in binary format. The binary format of an entry is as follows:
 * <ul>
 *   <li>Required 32-bit signed entry length</li>
 *   <li>Required 32-bit unsigned entry checksum</li>
 *   <li>Required 64-bit signed offset</li>
 *   <li>Required 8-bit term flag</li>
 *   <li>Optional 64-bit term</li>
 * </ul>
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class Segment implements AutoCloseable {
  private final SegmentFile file;
  private final SegmentDescriptor descriptor;
  private final Serializer serializer;
  private final Buffer buffer;
  private final HeapBuffer memory = HeapBuffer.allocate();
  private final OffsetIndex offsetIndex;
  private final OffsetPredicate offsetPredicate;
  private final TermIndex termIndex = new TermIndex();
  private final SegmentManager manager;
  private long skip = 0;
  private boolean open = true;

  /**
   * @throws NullPointerException if any argument is null
   */
  Segment(SegmentFile file, Buffer buffer, SegmentDescriptor descriptor, OffsetIndex offsetIndex, OffsetPredicate offsetPredicate, Serializer serializer, SegmentManager manager) {
    this.serializer = Assert.notNull(serializer, "serializer");
    this.file = Assert.notNull(file, "file");
    this.buffer = Assert.notNull(buffer, "buffer");
    this.descriptor = Assert.notNull(descriptor, "descriptor");
    this.offsetIndex = Assert.notNull(offsetIndex, "offsetIndex");
    this.offsetPredicate = Assert.notNull(offsetPredicate, "offsetPredicate");
    this.manager = Assert.notNull(manager, "manager");
    buildIndex();
  }

  /**
   * Builds the index from the segment bytes.
   */
  private void buildIndex() {
    // Read the current buffer position.
    long position = buffer.mark().position();

    // Read the first entry length.
    int length = buffer.readInt();

    // While the length is non-zero...
    while (length != 0) {
      // Read the full entry into memory.
      buffer.read(memory.clear().limit(length));

      // Flip the in-memory buffer.
      memory.flip();

      // Read the 64-bit entry checksum.
      long checksum = memory.readUnsignedInt();

      // Read the 64-bit entry offset.
      long offset = memory.readLong();

      // If the term is set on the entry, read the term.
      Long term = memory.readBoolean() ? memory.readLong() : null;

      // Calculate the entry position and length.
      int entryPosition = (int) memory.position();
      int entryLength = length - entryPosition;

      // Compute the checksum for the entry bytes.
      Checksum crc32 = new CRC32();
      crc32.update(memory.array(), entryPosition, entryLength);

      // If the computed checksum equals the stored checksum...
      if (checksum == crc32.getValue()) {
        // If the entry contained a term, index the term.
        if (term != null) {
          termIndex.index(offset, term);
        }

        // Index the entry offset.
        offsetIndex.index(offset, position);
      } else {
        break;
      }

      // Store the next entry start position.
      position = buffer.position();

      // Read the next entry length.
      length = buffer.mark().readInt();
    }

    // Reset the buffer back to the start of the next entry.
    buffer.reset();
  }

  /**
   * Returns the segment file.
   *
   * @return The segment file.
   */
  public SegmentFile file() {
    return file;
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

    // Get the term from the entry.
    long term = entry.getTerm();

    // Get the highest term in the index.
    long lastTerm = termIndex.term();

    // The entry term must be positive and >= the last term in the segment.
    Assert.arg(term > 0 && term >= lastTerm, "term must be monotonically increasing");

    // Mark the starting position of the record and record the starting position of the new entry.
    long position = buffer.position();

    // Determine whether to skip writing the term to the segment.
    boolean skipTerm = term == lastTerm;

    // Calculate the length of the entry header bytes.
    int headerLength = INTEGER + LONG + BOOLEAN + (skipTerm ? 0 : LONG);

    // Clear the memory and skip the size and header.
    memory.clear().skip(headerLength);

    // Serialize the object into the in-memory buffer.
    serializer.writeObject(entry, memory);

    // Flip the in-memory buffer indexes.
    memory.flip();

    // The total length of the entry is the in-memory buffer limit.
    int totalLength = (int) memory.limit();

    // Calculate the length of the serialized bytes based on the in-memory buffer limit and header length.
    int entryLength = totalLength - headerLength;

    // Set the entry size.
    entry.setSize(totalLength);

    // Compute the checksum for the entry.
    Checksum crc32 = new CRC32();
    crc32.update(memory.array(), headerLength, entryLength);
    long checksum = crc32.getValue();

    // Rewind the in-memory buffer and write the length, checksum, and offset.
    memory.rewind()
      .writeUnsignedInt(checksum)
      .writeLong(offset);

    // If the term has not yet been written, write the term to this entry.
    if (skipTerm) {
      memory.writeBoolean(false);
    } else {
      memory.writeBoolean(true).writeLong(term);
    }

    // Write the entry length and entry to the segment.
    buffer.writeInt(totalLength)
      .write(memory.rewind());

    // Index the offset, position, and length.
    offsetIndex.index(offset, position);

    // If the entry term is greater than the last indexed term, index the term.
    if (term > lastTerm) {
      termIndex.index(offset, term);
    }

    // Reset skip to zero since we wrote a new entry.
    skip = 0;

    return index;
  }

  /**
   * Reads the term for the entry at the given index.
   *
   * @param index The index for which to read the term.
   * @return The term for the given index.
   * @throws IllegalStateException if the segment is not open or {@code index} is inconsistent
   */
  public long term(long index) {
    assertSegmentOpen();
    checkRange(index);

    // Get the offset of the index within this segment.
    long offset = relativeOffset(index);

    // Look up the term for the offset in the term index.
    return termIndex.lookup(offset);
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
      int length = buffer.readInt(position);

      // Read the entry into memory.
      try (Buffer slice = buffer.slice(position + INTEGER, length)) {
        slice.read(memory.clear().limit(length));
        memory.flip();
      }

      // Read the checksum of the entry.
      long checksum = memory.readUnsignedInt();

      // Verify that the entry at the given offset matches.
      long entryOffset = memory.readLong();
      Assert.state(entryOffset == offset, "inconsistent index: %s", index);

      // Skip the term if necessary.
      if (memory.readBoolean()) {
        memory.skip(LONG);
      }

      // Calculate the entry position and length.
      int entryPosition = (int) memory.position();
      int entryLength = length - entryPosition;

      // Compute the checksum for the entry bytes.
      Checksum crc32 = new CRC32();
      crc32.update(memory.array(), entryPosition, entryLength);

      // If the stored checksum equals the computed checksum, return the entry.
      if (checksum == crc32.getValue()) {
        T entry = serializer.readObject(memory);
        entry.setIndex(index).setTerm(termIndex.lookup(offset)).setSize(length);
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
   * Releases an entry from the segment.
   *
   * @param index The index of the entry to release.
   * @return Indicates whether the entry was newly released from the segment.
   * @throws IllegalStateException if the segment is not open
   */
  public boolean release(long index) {
    assertSegmentOpen();
    long offset = offsetIndex.find(relativeOffset(index));
    return offset != -1 && offsetPredicate.release(offset);
  }

  /**
   * Returns a boolean value indicating whether the given index was released from the segment.
   *
   * @param index The index of the entry to check.
   * @return Indicates whether the given entry was released from the segment.
   * @throws IllegalStateException if the segment is not open
   */
  public boolean isLive(long index) {
    assertSegmentOpen();
    return offsetPredicate.test(offsetIndex.find(relativeOffset(index)));
  }

  /**
   * Returns the number of entries in the segment that have been released.
   *
   * @return The number of entries in the segment that have been released.
   * @throws IllegalStateException if the segment is not open
   */
  public long releaseCount() {
    assertSegmentOpen();
    return offsetPredicate.count();
  }

  /**
   * Returns a predicate for live offsets in the segment.
   *
   * @return A predicate for live offsets in the segment.
   */
  public OffsetPredicate offsetPredicate() {
    return offsetPredicate;
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
      termIndex.truncate(offset);
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
    offsetPredicate.close();
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
