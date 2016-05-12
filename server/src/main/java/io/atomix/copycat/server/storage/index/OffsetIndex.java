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
package io.atomix.copycat.server.storage.index;

import io.atomix.copycat.server.storage.Segment;

/**
 * Segment offset index.
 * <p>
 * The offset index handles indexing of entries in a given {@link Segment}. Given the offset and position of an entry
 * in a segment, the index will write the position to an underlying {@link io.atomix.catalyst.buffer.Buffer}. With this information, the index provides
 * useful metadata about the log such as the number of physical entries in the log and the first and last offsets.
 * <p>
 * Each entry in the index is stored in 8 bytes, a 1 byte status flag, a 24-bit unsigned offset, and a 32-bit unsigned
 * position. This places a limitation on the maximum indexed offset at {@code 2^31 - 1} and maximum indexed position at
 * {@code 2^32 - 1}.
 * <p>
 * When the index is first created, the {@link io.atomix.catalyst.buffer.Buffer} provided to the constructor will be scanned for existing entries.
 * <p>
 * The index assumes that entries will always be indexed in increasing order. However, this index also allows arbitrary
 * entries to be missing from the log due to log compaction. Because of the potential for missing entries, binary search
 * is used to locate positions rather than absolute positions. For efficiency, a {@link io.atomix.catalyst.buffer.MappedBuffer}
 * can be used to improve the performance of the binary search algorithm for persistent indexes.
 * <p>
 * In order to prevent searching the index for missing entries, all offsets are added to a memory efficient {@link io.atomix.catalyst.buffer.util.BitArray}
 * as they're written to the index. The bit array is sized according to the underlying index buffer. Prior to searching
 * for an offset in the index, the {@link io.atomix.catalyst.buffer.util.BitArray} is checked for existence of the offset in the index. Only if the offset
 * exists in the index is a binary search required.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface OffsetIndex extends AutoCloseable {

  /**
   * Returns the count of the index for the given number of entries.
   */
  static long size(int maxEntries) {
    return (long) maxEntries * 12 + 16;
  }

  /**
   * Returns the last offset in the index.
   */
  long lastOffset();

  /**
   * Indexes the given offset with the given position.
   *
   * @param offset The offset to index.
   * @param position The position of the offset to index.
   * @return Indicates whether the offset was successfully indexed.
   * @throws IllegalArgumentException if the {@code offset} is less than or equal to the last offset in the index, 
   * or {@code position} is greater than MAX_POSITION
   */
  boolean index(long offset, long position);

  /**
   * Returns a boolean value indicating whether the index is empty.
   *
   * @return Indicates whether the index is empty.
   */
  boolean isEmpty();

  /**
   * Returns the number of entries active in the index.
   *
   * @return The number of entries active in the index.
   */
  int size();

  /**
   * Returns a boolean value indicating whether the index contains the given offset.
   *
   * @param offset The offset to check.
   * @return Indicates whether the index contains the given offset.
   */
  boolean contains(long offset);

  /**
   * Finds the starting position of the given offset.
   *
   * @param offset The offset to look up.
   * @return The starting position of the given offset.
   */
  long position(long offset);

  /**
   * Finds the real offset for the given relative offset.
   */
  long find(long offset);

  /**
   * Truncates the index up to the given offset.
   * <p>
   * This method assumes that the given offset is contained within the index. If the offset is not indexed then the
   * index will not be truncated.
   *
   * @param offset The offset after which to truncate the index.
   * @return The position of the last entry in the index.
   */
  long truncate(long offset);

  /**
   * Flushes the index to the underlying storage.
   */
  void flush();

  /**
   * Closes the index.
   */
  @Override
  void close();

  /**
   * Deletes the index.
   */
  void delete();

}
