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
import io.atomix.catalyst.buffer.MappedBuffer;
import io.atomix.catalyst.util.Assert;

/**
 * Sequential offset index.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SequentialOffsetIndex implements OffsetIndex {
  private static final long MAX_POSITION = (long) Math.pow(2, 32) - 1;
  private static final int POSITION_SIZE = 4;
  private final Buffer buffer;
  private int size;
  private long lastOffset = -1;

  /**
   * @throws NullPointerException if {@code buffer} is null
   */
  public SequentialOffsetIndex(Buffer buffer) {
    this.buffer = Assert.notNull(buffer, "buffer");
  }

  @Override
  public long lastOffset() {
    return lastOffset;
  }

  @Override
  public boolean index(long offset, long position) {
    Assert.argNot(offset, lastOffset > -1 && offset <= lastOffset, "offset cannot be less than or equal to the last offset in the index");
    Assert.argNot(position > MAX_POSITION, "position cannot be greater than " + MAX_POSITION);
    if (offset > lastOffset + 1) {
      return false;
    } else if (offset == lastOffset + 1) {
      buffer.writeUnsignedInt(position);
      size++;
      lastOffset = offset;
    }
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
    return offset >= 0 && offset <= lastOffset;
  }

  @Override
  public long position(long offset) {
    if (offset >= 0 && offset <= lastOffset) {
      return buffer.readUnsignedInt(offset * POSITION_SIZE);
    }
    return -1;
  }

  @Override
  public long find(long offset) {
    return offset;
  }

  @Override
  public long truncate(long offset) {
    if (offset == lastOffset) {
      return -1;
    }

    if (offset == -1) {
      buffer.position(0).zero();
      lastOffset = -1;
      size = 0;
      return 0;
    }

    buffer.position(offset * POSITION_SIZE + POSITION_SIZE);
    lastOffset = offset;
    size = (int)(lastOffset + 1);
    return buffer.readUnsignedInt(offset * POSITION_SIZE + POSITION_SIZE);
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
