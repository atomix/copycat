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
 * Log writer.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class LogWriter implements Writer {
  private final SegmentManager segments;
  private volatile Segment currentSegment;
  private volatile SegmentWriter currentWriter;

  public LogWriter(SegmentManager segments) {
    this.segments = segments;
    this.currentSegment = segments.lastSegment();
    this.currentWriter = currentSegment.writer();
  }

  @Override
  public LogWriter lock() {
    // TODO
    return this;
  }

  @Override
  public LogWriter unlock() {
    // TODO
    return this;
  }

  @Override
  public long lastIndex() {
    return currentWriter.lastIndex();
  }

  @Override
  public Indexed<? extends Entry<?>> lastEntry() {
    return currentWriter.lastEntry();
  }

  @Override
  public <T extends Entry<T>> Indexed<T> append(Indexed<T> entry) {
    return null;
  }

  @Override
  public long nextIndex() {
    return currentWriter.nextIndex();
  }

  /**
   * Commits entries up to the given index.
   *
   * @param index The index up to which to commit entries.
   * @return The log writer.
   */
  public LogWriter commit(long index) {
    segments.commitIndex(index);
    return this;
  }

  @Override
  public Writer skip() {
    return currentWriter.skip();
  }

  @Override
  public Writer skip(long entries) {
    return currentWriter.skip(entries);
  }

  @Override
  public <T extends Entry<T>> Indexed<T> append(long term, T entry) {
    if (currentWriter.isFull()) {
      currentSegment = segments.nextSegment();
      currentWriter = currentSegment.writer();
    }
    return currentWriter.append(term, entry);
  }

  @Override
  public LogWriter truncate(long index) {
    // Delete all segments with first indexes greater than the given index.
    while (index < currentWriter.firstIndex()) {
      currentWriter.close();
      segments.removeSegment(currentSegment);
      currentSegment = segments.lastSegment();
      currentWriter = currentSegment.writer();
    }

    // Truncate the current index.
    currentWriter.truncate(index);
    return this;
  }

  @Override
  public LogWriter flush() {
    currentWriter.flush();
    return this;
  }

  @Override
  public void close() {
    currentWriter.close();
  }
}
