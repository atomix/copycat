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

/**
 * Entry cleaner.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class EntryCleaner {
  volatile long offset;
  volatile SegmentCleaner cleaner;

  public EntryCleaner(long offset, SegmentCleaner cleaner) {
    this.offset = offset;
    this.cleaner = cleaner;
  }

  /**
   * Updates the entry offset and segment cleaner.
   *
   * @param offset The entry offset.
   * @param cleaner The segment cleaner.
   */
  synchronized void update(long offset, SegmentCleaner cleaner) {
    this.offset = offset;
    this.cleaner = cleaner;
  }

  /**
   * Cleans the entry.
   */
  public synchronized void clean() {
    cleaner.clean(offset);
  }

  /**
   * Returns a boolean indicating whether the entry is clean.
   *
   * @return Indicates whether the entry is clean.
   */
  boolean isClean() {
    return cleaner.isClean(offset);
  }

  @Override
  public String toString() {
    return String.format("%s(offset=%d)", getClass().getSimpleName(), offset);
  }
}
