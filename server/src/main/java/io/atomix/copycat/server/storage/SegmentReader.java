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
import io.atomix.copycat.server.storage.buffer.Buffer;
import io.atomix.copycat.server.storage.buffer.HeapBuffer;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.util.concurrent.ReferenceManager;

import java.util.HashSet;
import java.util.Set;

/**
 * Segment reader.
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
public class SegmentReader implements Reader, ReferenceManager<Indexed<?>> {
  private final Segment segment;
  private final Buffer buffer;
  private final boolean commitsOnly;
  private final HeapBuffer memory = HeapBuffer.allocate();
  private final Kryo serializer = new Kryo();
  private long firstIndex;
  private int currentOffset = -1;
  private long currentIndex;
  private Indexed<? extends Entry<?>> currentEntry;
  private long currentTerm;
  private final Set<Indexed<?>> entries = new HashSet<>();

  public SegmentReader(Segment segment, Buffer buffer, boolean commitsOnly) {
    this.segment = segment;
    this.buffer = buffer;
    this.commitsOnly = commitsOnly;
  }

  @Override
  public Reader lock() {
    // TODO
    return this;
  }

  @Override
  public Reader unlock() {
    // TODO
    return this;
  }

  /**
   * Returns the first index in the segment.
   *
   * @return The first index in the segment.
   */
  public long firstIndex() {
    return firstIndex;
  }

  @Override
  public long currentIndex() {
    return currentIndex;
  }

  @Override
  public long nextIndex() {
    return buffer.readLong(buffer.position());
  }

  @Override
  public Indexed<? extends Entry<?>> entry() {
    return currentEntry;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Entry<T>> Indexed<T> get(long index) {
    if (index > segment.manager().commitIndex()) {
      throw new IllegalStateException("Cannot read uncommitted entries");
    }

    reset(index);

    // If the current index is equal to the given index, return the current entry.
    // It's possible that the current entry is null.
    if (index == currentIndex) {
      return (Indexed<T>) currentEntry;
    }
    return null;
  }

  @Override
  public Indexed<? extends Entry<?>> reset(long index) {
    // If the reset index is the current index, do nothing.
    if (index == currentIndex) {
      return entry();
    }

    // If the given index is less than the current index, reset the buffer.
    if (index < currentIndex) {
      reset();
    }

    // Scan the segment up to the given index.
    scan(index);
    return entry();
  }

  @Override
  public void reset() {
    buffer.clear();
    currentOffset = -1;
    currentIndex = 0;
    currentTerm = 0;
    firstIndex = buffer.readLong(0);
  }

  /**
   * Scans the segment up to the given index.
   *
   * @param index The index to which to scan the segment.
   */
  private void scan(long index) {
    while (nextIndex() <= index) {
      next();
    }
  }

  @Override
  public boolean hasNext() {
    if (commitsOnly) {
      return nextIndex() < segment.manager().commitIndex();
    } else {
      return nextIndex() > 0;
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public Indexed<? extends Entry<?>> next() {
    // Read the index from the entry.
    currentIndex = buffer.readLong();

    // Increment the current offset.
    currentOffset++;

    // If the first index is not yet set, set it.
    if (firstIndex == 0) {
      firstIndex = currentIndex;
    }

    // Read the byte indicating whether a term change is stored in this entry and read the term if necessary.
    if (buffer.readBoolean()) {
      currentTerm = buffer.readLong();
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
    currentEntry = new Indexed(currentIndex, currentTerm, entry, length + Long.BYTES + Long.BYTES, this, new EntryCleaner(currentOffset, segment.cleaner()));
    entries.add(currentEntry);
    return currentEntry;
  }

  @Override
  public void release(Indexed<?> reference) {
    entries.remove(reference);
  }

  @Override
  public void close() {
    memory.close();
    buffer.close();
  }
}
