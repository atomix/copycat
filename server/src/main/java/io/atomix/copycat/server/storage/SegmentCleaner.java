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

import io.atomix.copycat.util.buffer.HeapBytes;
import io.atomix.copycat.server.storage.compaction.Compaction;
import io.atomix.copycat.util.Assert;

/**
 * Segment cleaner.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SegmentCleaner {
  private final HeapBytes bytes;
  private long size;
  private long count;

  public SegmentCleaner() {
    this(HeapBytes.allocate(1024));
  }

  public SegmentCleaner(HeapBytes bytes) {
    this.bytes = bytes;
    this.size = bytes.size() * 4; // Four entries per byte
  }

  /**
   * Transfers the given entry to the cleaner.
   *
   * @param entry The entry to transfer.
   */
  void transfer(long offset, Indexed<?> entry) {
    if (entry.cleaner != null) {
      entry.cleaner.update(offset, this);
    }
  }

  /**
   * Returns the compaction mode for the given offset.
   *
   * @param offset The offset to check.
   * @return The compaction mode for the given offset.
   */
  public Compaction.Mode get(long offset) {
    if (offset > size) {
      throw new IndexOutOfBoundsException();
    }

    final long mode = (bytes.readLong(position(offset)) >> index(offset)) & 3l;
    return Compaction.Mode.valueOf((int) mode);
  }

  /**
   * Releases an offset from the segment.
   *
   * @param offset The offset to release.
   * @param mode The compaction mode with which to compact the entry at the given offset.
   * @return Indicates whether the offset was newly released.
   */
  public boolean set(long offset, Compaction.Mode mode) {
    Assert.argNot(offset < 0, "offset must be positive");
    if (size <= offset) {
      while (size <= offset) {
        resize(size * 2);
      }
    }

    if (get(offset) == Compaction.Mode.NONE) {
      final long position = position(offset);
      bytes.writeLong(position, bytes.readLong(position) | (mode.mask << index(offset)));
      count++;
      return true;
    }
    return false;
  }

  /**
   * Returns the position of the long that stores the bits for the given offset.
   */
  private long position(long offset) {
    return (offset / 32) * 8;
  }

  /**
   * Returns the index of the bit for the given offset.
   */
  private long index(long offset) {
    return offset * 2 % 64;
  }

  /**
   * Resizes the cleaner bit array to the given size.
   *
   * @param size The size to which to resize the bit array.
   */
  private void resize(long size) {
    bytes.resize(Math.max(size / 4 + 8, 8));
    this.size = size;
  }

  /**
   * Returns the number of offsets released from the segment.
   *
   * @return The number of offsets released from the segment.
   */
  public long count() {
    return count;
  }

  /**
   * Copies the offset predicate.
   *
   * @return The copied offset predicate.
   */
  public SegmentCleaner copy() {
    return new SegmentCleaner(bytes.copy());
  }

  @Override
  public String toString() {
    return String.format("%s[size=%d, count=%d]", getClass().getSimpleName(), size, count);
  }
}
