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

import io.atomix.copycat.server.storage.buffer.Buffer;
import io.atomix.copycat.server.storage.buffer.FileBuffer;
import io.atomix.copycat.server.storage.buffer.MappedBuffer;
import io.atomix.copycat.server.storage.buffer.SlicedBuffer;

/**
 * Log segment.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Segment implements AutoCloseable {
  private final SegmentFile file;
  private final SegmentDescriptor descriptor;
  private final SegmentManager manager;
  private final Buffer buffer;
  private final SegmentCleaner cleaner = new SegmentCleaner();
  private final SegmentWriter writer;
  private volatile boolean open = true;

  public Segment(SegmentFile file, SegmentDescriptor descriptor, SegmentManager manager) {
    this.file = file;
    this.descriptor = descriptor;
    this.manager = manager;
    this.buffer = manager.openSegment(descriptor, "rw");
    this.writer = new SegmentWriter(this, buffer);
  }

  /**
   * Returns the segment manager.
   *
   * @return The segment manager.
   */
  SegmentManager manager() {
    return manager;
  }

  /**
   * Returns the segment ID.
   *
   * @return The segment ID.
   */
  public long id() {
    return descriptor.id();
  }

  /**
   * Returns the segment version.
   *
   * @return The segment version.
   */
  public long version() {
    return descriptor.version();
  }

  /**
   * Returns the segment's starting index.
   *
   * @return The segment's starting index.
   */
  public long index() {
    return descriptor.index();
  }

  /**
   * Returns the last index in the segment.
   *
   * @return The last index in the segment.
   */
  public long lastIndex() {
    return writer.lastIndex();
  }

  /**
   * Returns the segment file.
   *
   * @return The segment file.
   */
  public SegmentFile file() {
    return file;
  }

  /**
   * Returns the segment descriptor.
   *
   * @return The segment descriptor.
   */
  public SegmentDescriptor descriptor() {
    return descriptor;
  }

  /**
   * Returns the segment size.
   *
   * @return The segment size.
   */
  public long size() {
    return writer.size();
  }

  /**
   * Returns a boolean value indicating whether the segment is empty.
   * <p>
   * The segment is considered empty if no entries have been written to the segment and no indexes in the
   * segment have been {@link SegmentWriter#skip(long) skipped}.
   *
   * @return Indicates whether the segment is empty.
   */
  public boolean isEmpty() {
    return length() == 0;
  }

  /**
   * Returns a boolean indicating whether the segment is full.
   *
   * @return Indicates whether the segment is full.
   */
  public boolean isFull() {
    return writer.isFull();
  }

  /**
   * Returns a boolean value indicating whether the segment has been compacted.
   * <p>
   * The segment is considered compacted if its {@link SegmentDescriptor#version()} is greater than {@code 1}.
   *
   * @return Indicates whether the segment has been compacted.
   */
  public boolean isCompacted() {
    return descriptor.version() > 1;
  }

  /**
   * Returns the total number of entries on disk in the segment.
   *
   * @return The total number of entries on disk in the segment.
   */
  public int count() {
    return writer.count();
  }

  /**
   * Returns the segment length.
   *
   * @return The segment length.
   */
  public long length() {
    return writer.nextIndex() - index();
  }

  /**
   * Returns the segment cleaner.
   *
   * @return The segment cleaner.
   */
  public SegmentCleaner cleaner() {
    return cleaner;
  }

  /**
   * Returns the segment writer.
   *
   * @return The segment writer.
   */
  public SegmentWriter writer() {
    checkOpen();
    return writer;
  }

  /**
   * Creates a new segment reader.
   *
   * @param mode The mode in which to open the segment reader.
   * @return A new segment reader.
   */
  public SegmentReader createReader(Reader.Mode mode) {
    checkOpen();
    return new SegmentReader(this, manager.openSegment(descriptor, "r"), mode);
  }

  /**
   * Checks whether the segment is open.
   */
  private void checkOpen() {
    if (!open) {
      throw new IllegalStateException("segment not open");
    }
  }

  /**
   * Closes the segment.
   */
  @Override
  public void close() {
    buffer.close();
    descriptor.close();
    open = false;
  }

  /**
   * Deletes the segment.
   */
  public void delete() {
    Buffer buffer = this.buffer instanceof SlicedBuffer ? ((SlicedBuffer) this.buffer).root() : this.buffer;
    if (buffer instanceof FileBuffer) {
      ((FileBuffer) buffer).delete();
    } else if (buffer instanceof MappedBuffer) {
      ((MappedBuffer) buffer).delete();
    }
  }
}
