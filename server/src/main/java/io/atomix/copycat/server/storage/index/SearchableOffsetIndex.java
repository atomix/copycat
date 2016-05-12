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
package io.atomix.copycat.server.storage.index;

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.FileBuffer;
import io.atomix.catalyst.buffer.HeapBuffer;
import io.atomix.catalyst.buffer.MappedBuffer;
import io.atomix.catalyst.util.Assert;

/**
 * Ordered offset index;
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class SearchableOffsetIndex implements OffsetIndex {
  private static final long MAX_POSITION = (long) Math.pow(2, 32) - 1;
  private static final int OFFSET_SIZE = 8;
  private static final int POSITION_SIZE = 4;
  private static final int ENTRY_SIZE = OFFSET_SIZE + POSITION_SIZE;

  private final Buffer buffer;
  private int size;
  private long lastOffset = -1;
  private long currentOffset = -1;
  private long currentMatch = -1;

  /**
   * @throws NullPointerException if {@code buffer} is null
   */
  public SearchableOffsetIndex(OffsetIndex index) {
    this.buffer = HeapBuffer.allocate();
    for (long i = 0; i < index.size(); i++) {
      this.buffer.writeLong(i).writeUnsignedInt(index.position(i));
      size++;
    }
  }

  @Override
  public long lastOffset() {
    return lastOffset;
  }

  @Override
  public synchronized boolean index(long offset, long position) {
    Assert.argNot(offset, lastOffset > -1 && offset <= lastOffset, "offset cannot be less than or equal to the last offset in the index");
    Assert.argNot(position > MAX_POSITION, "position cannot be greater than " + MAX_POSITION);

    buffer.writeLong(offset).writeUnsignedInt(position);

    size++;
    lastOffset = offset;
    return true;
  }

  @Override
  public boolean isEmpty() {
    return size == 0;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean contains(long offset) {
    return position(offset) != -1;
  }

  @Override
  public synchronized long position(long offset) {
    long relativeOffset = find(offset);
    return relativeOffset != -1 ? buffer.readUnsignedInt(relativeOffset * ENTRY_SIZE + OFFSET_SIZE) : -1;
  }

  @Override
  public synchronized long find(long offset) {
    if (size == 0) {
      return -1;
    }

    if (offset == currentOffset) {
      return currentMatch;
    } else if (currentOffset != -1 && currentMatch + 1 < size && buffer.readLong((currentMatch + 1) * ENTRY_SIZE) == offset) {
      currentOffset = offset;
      return ++currentMatch;
    }

    int lo = 0;
    int hi = size - 1;

    while (lo < hi) {
      int mid = lo + (hi - lo) / 2;
      long i = buffer.readLong(mid * ENTRY_SIZE);
      if (i == offset) {
        currentOffset = offset;
        currentMatch = mid;
        return mid;
      } else if (lo == mid) {
        i = buffer.readLong(hi * ENTRY_SIZE);
        if (i == offset) {
          currentOffset = offset;
          currentMatch = hi;
          return hi;
        }
        return -1;
      } else if (i < offset) {
        lo = mid;
      } else {
        hi = mid - 1;
      }
    }

    if (buffer.readLong(hi * ENTRY_SIZE) == offset) {
      currentOffset = offset;
      currentMatch = hi;
      return hi;
    }
    return -1;
  }

  /**
   * Returns the real offset for the given relative offset.
   */
  private long findAfter(long offset) {
    if (size == 0) {
      return -1;
    }

    int low  = 0;
    int high = size-1;

    while (low <= high) {
      int mid = low + ((high - low) / 2);

      long i = buffer.readLong(mid * ENTRY_SIZE);
      if (i < offset) {
        low = mid + 1;
      } else if (i > offset) {
        high = mid - 1;
      } else {
        return mid;
      }
    }

    return (low < high) ? low + 1 : high + 1;
  }

  @Override
  public long truncate(long offset) {
    if (offset == lastOffset)
      return -1;

    if (offset == -1) {
      buffer.position(0).zero();
      currentOffset = currentMatch = lastOffset = -1;
      return 0;
    }

    long nearestOffset = findAfter(offset + 1);

    if (nearestOffset == -1)
      return -1;

    long nearestIndex = nearestOffset * ENTRY_SIZE;

    long lastOffset = lastOffset();
    for (long i = lastOffset; i > offset; i--) {
      if (position(i) != -1) {
        size--;
      }
    }

    long position = buffer.readUnsignedInt(nearestIndex + OFFSET_SIZE);

    buffer.position(nearestIndex)
      .zero(nearestIndex);
    this.lastOffset = offset;
    currentOffset = currentMatch = -1;

    return position;
  }

  @Override
  public void flush() {
    buffer.flush();
  }

  @Override
  public void close() {
    buffer.close();
  }

  @Override
  public void delete() {
    if (buffer instanceof FileBuffer) {
      ((FileBuffer) buffer).delete();
    } else if (buffer instanceof MappedBuffer) {
      ((MappedBuffer) buffer).delete();
    }
  }

}
