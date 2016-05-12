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

/**
 * Offset index that delegates to other indexes.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class DelegatingOffsetIndex implements OffsetIndex {
  private volatile OffsetIndex index;

  public DelegatingOffsetIndex(Buffer buffer) {
    this.index = new SequentialOffsetIndex(buffer);
  }

  @Override
  public long lastOffset() {
    return index.lastOffset();
  }

  @Override
  public boolean index(long offset, long position) {
    if (!index.index(offset, position)) {
      index = new SearchableOffsetIndex(index);
      return index.index(offset, position);
    }
    return true;
  }

  @Override
  public boolean isEmpty() {
    return index.isEmpty();
  }

  @Override
  public int size() {
    return index.size();
  }

  @Override
  public boolean contains(long offset) {
    return index.contains(offset);
  }

  @Override
  public long position(long offset) {
    return index.position(offset);
  }

  @Override
  public long find(long offset) {
    return index.find(offset);
  }

  @Override
  public long truncate(long offset) {
    return index.truncate(offset);
  }

  @Override
  public void flush() {
    index.flush();
  }

  @Override
  public void close() {
    index.close();
  }

  @Override
  public void delete() {
    index.delete();
  }
}
