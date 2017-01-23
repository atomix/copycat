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
public interface Writer extends AutoCloseable {

  /**
   * Acquires the writer lock.
   *
   * @return The writer.
   */
  Writer lock();

  /**
   * Releases the writer lock.
   *
   * @return The writer.
   */
  Writer unlock();

  /**
   * Returns the last written index.
   *
   * @return The last written index.
   */
  long lastIndex();

  /**
   * Returns the last entry written.
   *
   * @return The last entry written.
   */
  Indexed<? extends Entry<?>> lastEntry();

  /**
   * Returns the next index to be written.
   *
   * @return The next index to be written.
   */
  long nextIndex();

  /**
   * Skips {@code entries} entries in the log.
   *
   * @param entries The number of entries to skip.
   * @return The writer.
   */
  Writer skip(long entries);

  /**
   * Appends an indexed entry to the log.
   *
   * @param entry The indexed entry to append.
   * @param <T> The entry type.
   * @return The appended indexed entry.
   */
  <T extends Entry<T>> Indexed<T> append(Indexed<T> entry);

  /**
   * Appends an entry to the writer.
   *
   * @param term The term in which to append the entry.
   * @param entry The entry to append.
   * @param <T> The entry type.
   * @return The indexed entry.
   */
  <T extends Entry<T>> Indexed<T> append(long term, T entry);

  /**
   * Truncates the log to the given index.
   *
   * @param index The index to which to truncate the log.
   * @return The updated writer.
   */
  Writer truncate(long index);

  /**
   * Flushes written entries to disk.
   *
   * @return The flushed writer.
   */
  Writer flush();

  @Override
  void close();
}
