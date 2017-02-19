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

import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.util.buffer.Buffer;
import io.atomix.copycat.util.buffer.HeapBuffer;

import java.util.NoSuchElementException;

/**
 * Log segment reader.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SegmentReader implements Reader {
  private final Segment segment;
  private final Buffer buffer;
  private final Mode mode;
  private final HeapBuffer memory = HeapBuffer.allocate();
  private volatile long firstIndex;
  private volatile int nextOffset = 0;
  private volatile Indexed<? extends Entry<?>> currentEntry;
  private volatile Indexed<? extends Entry<?>> nextEntry;

  public SegmentReader(Segment segment, Buffer buffer, Mode mode) {
    this.segment = segment;
    this.buffer = buffer;
    this.mode = mode;
    readNext();
  }

  @Override
  public Mode mode() {
    return mode;
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
    return currentEntry != null ? currentEntry.index() : 0;
  }

  @Override
  public Indexed<? extends Entry<?>> currentEntry() {
    return currentEntry;
  }

  @Override
  public long nextIndex() {
    return nextEntry != null ? nextEntry.index() : 0;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Entry<T>> Indexed<T> get(long index) {
    // If the current entry is set, use it to determine whether to reset the reader.
    if (currentEntry != null) {
      // If the index matches the current entry index, return the current entry.
      if (index == currentEntry.index()) {
        return (Indexed<T>) currentEntry;
      }

      // If the index is less than the current entry index, reset the reader.
      if (index < currentEntry.index()) {
        reset();
      }
    }

    // Seek to the given index.
    while (hasNext()) {
      if (nextEntry.index() <= index) {
        next();
      } else {
        break;
      }
    }

    // If the current entry's index matches the given index, return it. Otherwise, return null.
    if (currentEntry != null && index == currentEntry.index()) {
      return (Indexed<T>) currentEntry;
    }
    return null;
  }

  @Override
  public Indexed<? extends Entry<?>> reset(long index) {
    return get(index);
  }

  @Override
  public void reset() {
    buffer.clear();
    currentEntry = null;
    nextOffset = 0;
    nextEntry = null;
    readNext();
  }

  @Override
  public boolean hasNext() {
    // If the next entry is null, check whether a next entry exists.
    if (nextEntry == null) {
      readNext();
    }
    return nextEntry != null;
  }

  @Override
  public Indexed<? extends Entry<?>> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    // Set the current entry to the next entry.
    currentEntry = nextEntry;

    // Reset the next entry to null.
    nextEntry = null;

    // Read the next entry in the segment.
    readNext();

    // Return the current entry.
    return currentEntry;
  }

  /**
   * Reads the next entry in the segment.
   */
  @SuppressWarnings("unchecked")
  private void readNext() {
    // Read the index for the next entry.
    long index = buffer.mark().readLong();

    long term = currentEntry != null ? currentEntry.term() : 0;

    // Loop through entries in the segment until a valid entry is found.
    while (index > 0) {

      // If the first index is not set, set it.
      if (firstIndex == 0) {
        firstIndex = index;
      }

      // If the reader is configured to only read commits, stop reading and return once the
      // reader reaches uncommitted entries.
      switch (mode) {
        case ALL_COMMITS:
        case LIVE_COMMITS:
        case UNCOMPACTED_COMMITS:
          if (index > segment.manager().commitIndex()) {
            buffer.reset();
            return;
          }
      }

      // If the entry contains a term, read the term.
      if (buffer.readBoolean()) {
        term = buffer.readLong();
      }

      // If the index is greater than 1 + the previous index, that indicates some entries have
      // been compacted from the segment. We need to determine whether any skipped entries should
      // be produced according to the configured read mode.
      for (long i = currentIndex() + 1; i < index; i++) {
        // Create the entry with a null value, indicating it has been compacted from the log.
        Indexed<? extends Entry<?>> entry = new Indexed<>(i, term, null, 0);

        // If the entry is valid according to the current read mode, reset the read buffer
        // and store the indexed entry as the next entry and return.
        if (mode.isValid(entry, segment.manager().compactor())) {
          buffer.reset();
          nextEntry = entry;
          return;
        }
      }

      // Read the length of the remainder of the entry.
      int length = buffer.readInt();

      // Read the entry bytes from the buffer into memory.
      buffer.read(memory.clear().limit(length));

      // Flip the in-memory buffer.
      memory.flip();

      // Read the entry type ID from the input.
      final int typeId = memory.readByte();

      // Look up the entry type.
      final Entry.Type<?> type = Entry.Type.forId(typeId);

      // Deserialize the entry.
      final Entry entry = type.serializer().readObject(memory, type.type());

      // If the index has been committed, create the entry with a cleaner.
      final Indexed<? extends Entry<?>> indexed;
      if (index <= segment.manager().commitIndex()) {
        indexed = new Indexed(index, term, entry, length, new EntryCleaner(++nextOffset, segment.cleaner()));
      }
      // Otherwise, the entry cannot be cleaned, but the offset still must be incremented.
      else {
        indexed = new Indexed(index, term, entry, length);
        nextOffset++;
      }

      // If the entry is valid for the current read mode, return.
      if (mode.isValid(indexed, segment.manager().compactor())) {
        nextEntry = indexed;
        return;
      }
      // Otherwise, read the next entry.
      else {
        index = buffer.mark().readLong();
      }
    }

    // If we've made it this far, reset the buffer to the last mark.
    buffer.reset();
  }

  @Override
  public void close() {
    memory.close();
    buffer.close();
  }
}
