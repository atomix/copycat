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

import java.util.Iterator;

/**
 * Log reader.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Reader extends Iterator<Indexed<? extends Entry<?>>>, AutoCloseable {

  /**
   * Acquires a lock on the reader.
   *
   * @return The locked reader.
   */
  Reader lock();

  /**
   * Releases a lock on the reader.
   *
   * @return The unlocked reader.
   */
  Reader unlock();

  /**
   * Returns the current reader index.
   *
   * @return The current reader index.
   */
  long currentIndex();

  /**
   * Returns the last read entry.
   *
   * @return The last read entry.
   */
  Indexed<? extends Entry<?>> entry();

  /**
   * Returns the next reader index.
   *
   * @return The next reader index.
   */
  long nextIndex();

  /**
   * Returns the entry at the given index.
   *
   * @param index The entry index.
   * @param <T> The entry type.
   * @return The entry at the given index or {@code null} if the entry doesn't exist.
   * @throws IndexOutOfBoundsException if the given index is outside the range of the log
   */
  <T extends Entry<T>> Indexed<T> get(long index);

  /**
   * Resets the reader to the given index.
   *
   * @param index The index to which to reset the reader.
   * @return The last entry read at the reset index.
   */
  Indexed<? extends Entry<?>> reset(long index);

  /**
   * Resets the reader to the start.
   */
  void reset();

  @Override
  void close();
}
