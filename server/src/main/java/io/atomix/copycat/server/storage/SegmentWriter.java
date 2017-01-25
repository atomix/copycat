/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.copycat.server.storage;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.server.storage.buffer.Buffer;
import io.atomix.copycat.server.storage.buffer.HeapBuffer;
import io.atomix.copycat.server.storage.entry.Entry;

/**
 * Segment writer.
 * <p>
 * The format of an entry in the log is as follows:
 * <ul>
 *   <li>64-bit index</li>
 *   <li>8-bit boolean indicating whether a term change is contained in the entry</li>
 *   <li>64-bit optional term</li>
 *   <li>32-bit signed entry length, including the entry type ID</li>
 *   <li>8-bit signed entry type ID</li>
 *   <li>n-bit entry bytes</li>
 * </ul>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SegmentWriter implements Writer {
  private final Segment segment;
  private final Buffer buffer;
  private final HeapBuffer memory = HeapBuffer.allocate();
  private final Kryo serializer = new Kryo();
  private long firstIndex;
  private Indexed<? extends Entry<?>> lastEntry;
  private long skip;
  private long lastTerm;
  private int entryCount;

  public SegmentWriter(Segment segment, Buffer buffer) {
    this.segment = segment;
    this.buffer = buffer;
    this.firstIndex = buffer.readLong(0);
    initialize();
  }

  /**
   * Initializes the writer by seeking to the end of the segment.
   */
  @SuppressWarnings("unchecked")
  private void initialize() {
    if (firstIndex == 0) {
      return;
    }

    // Read the index.
    long index = buffer.mark().readLong();

    // If the index exists, read the entry.
    while (index != 0) {
      // Read the byte indicating whether a term change is stored in this entry and read the term if necessary.
      if (buffer.readBoolean()) {
        lastTerm = buffer.readLong();
      }

      // Read the 32-bit entry length.
      final int length = buffer.readInt();

      // Read the entry bytes from the buffer into memory.
      buffer.read(memory.clear().limit(length));

      // Deserialize the entry into memory.
      final Input input = new Input(memory.array());

      // Read the entry type ID from the input.
      final int typeId = input.readByte();

      // Look up the entry type.
      final Entry.Type<?> type = Entry.Type.forId(typeId);

      // Deserialize the entry.
      final Entry entry = serializer.readObject(input, type.type(), type.serializer());

      // Return the indexed entry. We compute the size by the entry length plus index/term length.
      lastEntry = new Indexed<>(index, lastTerm, entry, length + Long.BYTES + Long.BYTES);

      // Mark the current position and read the next index.
      index = buffer.mark().readLong();
    }

    // Once we seek to the end of the segment, reset the buffer position to the last mark.
    buffer.reset();
  }

  /**
   * Returns the segment to which the writer belongs.
   *
   * @return The segment to which the writer belongs.
   */
  public Segment segment() {
    return segment;
  }

  @Override
  public Writer lock() {
    // TODO
    return this;
  }

  @Override
  public Writer unlock() {
    // TODO
    return this;
  }

  @Override
  public long lastIndex() {
    return lastEntry != null ? lastEntry.index() : segment.index() - 1;
  }

  @Override
  public Indexed<? extends Entry<?>> lastEntry() {
    return lastEntry;
  }

  @Override
  public long nextIndex() {
    if (lastEntry != null) {
      return lastEntry.index() + skip + 1;
    } else {
      return segment.index();
    }
  }

  /**
   * Returns the size of the underlying buffer.
   *
   * @return The size of the underlying buffer.
   */
  public long size() {
    return buffer.offset() + buffer.position();
  }

  /**
   * Returns the number of entries on disk in the segment.
   *
   * @return The number of entries on disk in the segment.
   */
  public int count() {
    return entryCount;
  }

  /**
   * Returns a boolean indicating whether the segment is full.
   *
   * @return Indicates whether the segment is full.
   */
  public boolean isFull() {
    return size() >= segment.descriptor().maxSegmentSize()
      || entryCount >= segment.descriptor().maxEntries();
  }

  /**
   * Returns the first index written to the segment.
   */
  public long firstIndex() {
    return firstIndex;
  }

  @Override
  public Writer skip() {
    return skip(1);
  }

  @Override
  public Writer skip(long entries) {
    skip += entries;
    return this;
  }

  /**
   * Appends an already indexed entry to the segment.
   *
   * @param entry The indexed entry to append.
   * @param <T> The entry type.
   * @return The updated indexed entry.
   */
  public <T extends Entry<T>> Indexed<T> append(Indexed<T> entry) {
    final long nextIndex = nextIndex();

    // If the entry's index is greater than the next index in the segment, skip some entries.
    if (entry.index() > nextIndex) {
      skip(entry.index() - nextIndex());
    }

    // If the entry's index is less than the next index, truncate the segment.
    if (entry.index() < nextIndex) {
      truncate(entry.index() - 1);
    }

    // Append the entry to the segment.
    Indexed<T> indexed = append(entry.term(), entry.entry());

    // Transfer the entry to the local segment cleaner.
    segment.cleaner().transfer(indexed.offset(), entry);

    // Return the original entry.
    return entry;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Entry<T>> Indexed<T> append(long term, T entry) {
    // Clear the in-memory buffer state.
    memory.clear();

    // Store the entry index.
    final long index = nextIndex();

    // Write the current index to the buffer.
    memory.writeLong(index);

    // If the term has changed, write a boolean flag and the new term. Otherwise, write false.
    if (term > lastTerm) {
      memory.writeBoolean(true).writeLong(term);
      this.lastTerm = term;
    } else {
      memory.writeBoolean(false);
    }

    // Mark the length position and skip the length bytes.
    final long lengthPosition = memory.position();
    memory.skip(Integer.BYTES);

    // Write the entry type ID.
    memory.writeByte(entry.type().id());

    // Serialize the entry into the buffer.
    Output output = new Output(memory.array());
    final int entryPosition = (int) memory.position();
    output.setPosition(entryPosition);
    serializer.writeObject(output, entry, entry.type().serializer());
    output.flush();

    // Compute the length of the entry by subtracting the entry start position from the end position
    // and adding the entry type ID byte.
    final int length = output.position() - entryPosition + Byte.BYTES;
    memory.writeInt(lengthPosition, length);

    // Write the in-memory entry buffer to the segment.
    buffer.write(memory.position(output.position()).flip());

    // Increment the entry count.
    entryCount++;

    // Return the indexed entry with the correct index/term/length.
    lastEntry = new Indexed<>(index, term, entry, length);
    return (Indexed<T>) lastEntry;
  }

  @Override
  @SuppressWarnings("unchecked")
  public SegmentWriter truncate(long index) {
    // If the index is greater than or equal to the last index, skip the truncate.
    if (index >= lastIndex()) {
      return this;
    }

    // If the index is less than the segment index, clear the segment buffer.
    if (index < segment.index()) {
      buffer.zero().clear();
      return this;
    }

    // Clear the write buffer to the beginning of the segment.
    buffer.clear();

    // Reset the entry count.
    entryCount = 0;

    // Read the index from the first entry.
    long lastIndex = buffer.readLong();

    // Skip the term flag and reset the current term, which will always exist in the first entry.
    lastTerm = buffer.skip(Byte.BYTES).readLong();

    // Read the length of the entry.
    int length = buffer.readInt();

    // Read the entry bytes from the buffer into memory.
    buffer.read(memory.clear().limit(length));

    // Deserialize the entry into memory.
    Input input = new Input(memory.array());

    // Read the entry type ID from the input.
    int typeId = input.readByte();

    // Look up the entry type.
    Entry.Type<?> type = Entry.Type.forId(typeId);

    // Deserialize the entry.
    Entry entry = serializer.readObject(input, type.type(), type.serializer());

    // Return the indexed entry. We compute the size by the entry length plus index/term length.
    lastEntry = new Indexed<>(lastIndex, lastTerm, entry, length + Long.BYTES + Long.BYTES);

    // Increment the entry count.
    entryCount++;

    // Mark the buffer to ensure we can reset it to the current index.
    buffer.mark();

    // Iterate through the entries until the truncate index is reached.
    while (lastIndex < index) {
      // Mark the current buffer position.
      buffer.mark();

      // Read the current index from the buffer.
      long currentIndex = buffer.readLong();

      // If the current index is greater than the truncate index, reset the position to the mark
      // and zero all bytes following it.
      if (currentIndex > index) {
        buffer.reset().zero(buffer.position());
        return this;
      }

      // Update the last index.
      lastIndex = currentIndex;

      // If a term change is contained in this entry, update the current term.
      if (buffer.readBoolean()) {
        lastTerm = buffer.readLong();
      }

      // Read the length of the entry.
      length = buffer.readInt();

      // Read the entry bytes from the buffer into memory.
      buffer.read(memory.clear().limit(length));

      // Deserialize the entry into memory.
      input = new Input(memory.array());

      // Read the entry type ID from the input.
      typeId = input.readByte();

      // Look up the entry type.
      type = Entry.Type.forId(typeId);

      // Deserialize the entry.
      entry = serializer.readObject(input, type.type(), type.serializer());

      // Return the indexed entry. We compute the size by the entry length plus index/term length.
      lastEntry = new Indexed<>(lastIndex, lastTerm, entry, length + Long.BYTES + Long.BYTES);

      // Increment the entry count.
      entryCount++;
    }

    // Zero the rest of the bytes in the segment.
    buffer.reset().zero(buffer.position());
    return this;
  }

  @Override
  public SegmentWriter flush() {
    buffer.flush();
    return this;
  }

  @Override
  public void close() {
    buffer.close();
  }
}
