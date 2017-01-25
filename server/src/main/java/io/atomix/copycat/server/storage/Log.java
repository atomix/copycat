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

import io.atomix.copycat.server.storage.compaction.Compactor;

import java.io.Closeable;

/**
 * Log.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Log implements Closeable {
  private final SegmentManager segments;
  private final LogWriter writer;
  private volatile boolean open;

  public Log(String name, Storage storage) {
    this.segments = new SegmentManager(name, storage);
    this.writer = new LogWriter(segments);
  }

  /**
   * Returns the log compactor.
   *
   * @return The log compactor.
   */
  public Compactor compactor() {
    return segments.compactor();
  }

  /**
   * Returns the log writer.
   *
   * @return The log writer.
   */
  public LogWriter writer() {
    return writer;
  }

  /**
   * Creates a new log reader.
   *
   * @param index The index at which to start the reader.
   * @param mode The mode in which to open the log reader.
   * @return A new log reader.
   */
  public LogReader createReader(long index, Reader.Mode mode) {
    return new LogReader(segments, index, mode);
  }

  /**
   * Returns a boolean indicating whether the log is open.
   *
   * @return Indicates whether the log is open.
   */
  public boolean isOpen() {
    return open;
  }

  @Override
  public void close() {
    segments.close();
    open = false;
  }
}
