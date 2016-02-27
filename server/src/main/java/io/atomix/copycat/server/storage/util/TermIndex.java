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
package io.atomix.copycat.server.storage.util;

import java.util.Map;
import java.util.TreeMap;

/**
 * Log entry term index.
 * <p>
 * The term index facilitates storing terms for a group of entries by relating the term only
 * to the first offset for all entries in the term. Because terms are monotonically increasing,
 * we can assume that if entry {@code n}'s term is {@code t} then entry {@code n + 1}'s term
 * will be {@code t} or greater.
 * <p>
 * The implementation of the term index uses a {@link TreeMap} to store a map of offsets to
 * terms. To look up the term for any given offset, we use {@code map.floorEntry(offset)}
 * to loop up the term for the offset.
 * <p>
 * This class is thread safe.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class TermIndex {
  private final TreeMap<Long, Long> terms = new TreeMap<>();

  /**
   * Returns the highest term in the index.
   *
   * @return The highest term in the index.
   */
  public synchronized long term() {
    Map.Entry<Long, Long> entry = terms.lastEntry();
    return entry != null ? entry.getValue() : 0;
  }

  /**
   * Indexes the given offset with the given term.
   *
   * @param offset The offset to index.
   * @param term The term to index.
   */
  public synchronized void index(long offset, long term) {
    if (lookup(offset) != term) {
      terms.put(offset, term);
    }
  }

  /**
   * Looks up the term for the given offset.
   *
   * @param offset The offset for which to look up the term.
   * @return The term for the entry at the given offset.
   */
  public synchronized long lookup(long offset) {
    Map.Entry<Long, Long> entry = terms.floorEntry(offset);
    return entry != null ? entry.getValue() : 0;
  }

  /**
   * Truncates the index to the given offset.
   *
   * @param offset The offset to which to truncate the index.
   */
  public synchronized void truncate(long offset) {
    terms.tailMap(offset, false).clear();
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
