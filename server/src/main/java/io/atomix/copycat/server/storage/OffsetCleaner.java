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
package io.atomix.copycat.server.storage;

import io.atomix.catalyst.buffer.util.BitArray;
import io.atomix.catalyst.util.Assert;

import java.util.function.Predicate;

/**
 * Segment offset cleaner.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class OffsetCleaner implements Predicate<Long>, AutoCloseable {
  private final BitArray bits;

  public OffsetCleaner() {
    this(BitArray.allocate(1024));
  }

  OffsetCleaner(BitArray bits) {
    this.bits = Assert.notNull(bits, "bits");
  }

  @Override
  public boolean test(Long offset) {
    return isClean(offset);
  }

  /**
   * Cleans an offset from the segment.
   *
   * @param offset The offset to clean.
   * @return Indicates whether the offset was newly cleaned.
   */
  public boolean clean(long offset) {
    Assert.argNot(offset < 0, "offset must be positive");
    if (bits.size() <= offset) {
      while (bits.size() <= offset) {
        bits.resize(bits.size() * 2);
      }
    }
    return bits.set(offset);
  }

  /**
   * Returns a boolean value indicating whether an offset has been cleaned from the segment.
   *
   * @param offset The offset to check.
   * @return Indicates whether the given offset has been cleaned from the segment.
   */
  public boolean isClean(long offset) {
    return offset == -1 || (bits.size() > offset && bits.get(offset));
  }

  /**
   * Returns the number of offsets cleaned from the segment.
   *
   * @return The number of offsets cleaned from the segment.
   */
  public long count() {
    return bits.count();
  }

  /**
   * Returns the underlying cleaner bit array.
   *
   * @return The underlying cleaner bit array.
   */
  BitArray bits() {
    return bits;
  }

  @Override
  public void close() {
    bits.close();
  }

}
