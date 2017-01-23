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

/**
 * Log reader.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class LogReader implements Reader {
  private final SegmentManager segments;
  private final boolean commitsOnly;
  private Segment currentSegment;
  private SegmentReader currentReader;

  public LogReader(SegmentManager segments, boolean commitsOnly) {
    this.segments = segments;
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

  @Override
  public long currentIndex() {
    return currentReader.currentIndex();
  }

  @Override
  public Indexed<? extends Entry<?>> entry() {
    return currentReader.entry();
  }

  @Override
  public long nextIndex() {
    if (hasNext()) {
      return currentReader.nextIndex();
    }
    return -1;
  }

  @Override
  public <T extends Entry<T>> Indexed<T> get(long index) {
    reset(index);
    return currentReader.get(index);
  }

  @Override
  public Indexed<? extends Entry<?>> reset(long index) {
    if (index < currentReader.firstIndex()) {
      currentSegment = segments.previousSegment(currentSegment.index());
      while (currentSegment != null) {
        currentReader.close();
        currentReader = currentSegment.createReader(commitsOnly);
        if (currentReader.firstIndex() < index) {
          break;
        }
      }
    }
    return currentReader.reset(index);
  }

  @Override
  public void reset() {
    currentSegment = segments.firstSegment();
    currentReader = currentSegment.createReader(commitsOnly);
  }

  @Override
  public boolean hasNext() {
    if (!currentReader.hasNext()) {
      Segment nextSegment = segments.nextSegment(currentSegment.index());
      if (nextSegment != null) {
        currentSegment = nextSegment;
        currentReader = currentSegment.createReader(commitsOnly);
      }
    }
    return currentReader.hasNext();
  }

  @Override
  public Indexed<? extends Entry<?>> next() {
    return currentReader.next();
  }

  @Override
  public void close() {
    currentReader.close();
  }
}
