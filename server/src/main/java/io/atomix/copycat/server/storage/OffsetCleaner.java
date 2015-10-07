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

/**
 * Segment cleaner.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class OffsetCleaner implements AutoCloseable {
  private final BitArray bits = BitArray.allocate(1024);

  /**
   * Cleans an offset from the segment.
   *
   * @param offset The offset to clean.
   * @return Indicates whether the offset was newly cleaned.
   */
  public boolean clean(int offset) {
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
  public boolean isClean(int offset) {
    return offset == -1 || (bits.size() > offset && bits.get(offset));
  }

  /**
   * Returns the number of offsets cleaned from the segment.
   *
   * @return The number of offsets cleaned from the segment.
   */
  public int count() {
    return (int) bits.count();
  }

  @Override
  public void close() {
    bits.close();
  }

}
