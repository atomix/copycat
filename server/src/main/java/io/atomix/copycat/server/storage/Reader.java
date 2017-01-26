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
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.Iterator;

/**
 * Log reader.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Reader extends Iterator<Indexed<? extends Entry<?>>>, AutoCloseable {

  /**
   * Reader mode.
   */
  enum Mode {
    /**
     * Reads all entries in the log, including {@code null} compacted entries.
     */
    ALL {
      @Override
      boolean isValid(Indexed<? extends Entry<?>> entry, Compactor compactor) {
        return true;
      }
    },

    /**
     * Reads all non-null entries in the log.
     */
    UNCOMPACTED {
      @Override
      boolean isValid(Indexed<? extends Entry<?>> entry, Compactor compactor) {
        return !entry.isCompacted();
      }
    },

    /**
     * Reads all live entries in the log.
     */
    LIVE {
      @Override
      boolean isValid(Indexed<? extends Entry<?>> entry, Compactor compactor) {
        if (entry.isCompacted()) {
          return false;
        }

        switch (entry.compaction()) {
          // SNAPSHOT entries are returned if the snapshotIndex is less than the entry index.
          case SNAPSHOT:
            return entry.index() > compactor.snapshotIndex();
          // RELEASE and QUORUM entries are returned if the minorIndex is less than the entry index or the
          // entry is still live.
          case QUORUM:
            return entry.index() > compactor.minorIndex();
          // SEQUENTIAL entries are returned if the minorIndex or majorIndex is less than the
          // entry index or if the entry is still live.
          case SEQUENTIAL:
            return entry.index() > compactor.minorIndex() || entry.index() > compactor.majorIndex();
          case NONE:
            return true;
        }
        return false;
      }
    },

    /**
     * Reads all committed entries in the log.
     */
    ALL_COMMITS {
      @Override
      boolean isValid(Indexed<? extends Entry<?>> entry, Compactor compactor) {
        return entry.isCommitted();
      }
    },

    /**
     * Reads all non-null committed entries.
     */
    UNCOMPACTED_COMMITS {
      @Override
      boolean isValid(Indexed<? extends Entry<?>> entry, Compactor compactor) {
        return entry.isCommitted() && !entry.isCompacted();
      }
    },

    /**
     * Reads all non-null committed entries that have not been cleaned.
     */
    LIVE_COMMITS {
      @Override
      boolean isValid(Indexed<? extends Entry<?>> entry, Compactor compactor) {
        if (!entry.isCommitted() || entry.isCompacted()) {
          return false;
        }

        switch (entry.compaction()) {
          // SNAPSHOT entries are returned if the snapshotIndex is less than the entry index.
          case SNAPSHOT:
            return entry.index() > compactor.snapshotIndex();
          // QUORUM entries are returned if the minorIndex is less than the entry index or the
          // entry is still live.
          case QUORUM:
            return entry.index() > compactor.minorIndex();
          // SEQUENTIAL entries are returned if the minorIndex or majorIndex is less than the
          // entry index or if the entry is still live.
          case SEQUENTIAL:
            return entry.index() > compactor.minorIndex() || entry.index() > compactor.majorIndex();
          case NONE:
            return true;
        }
        return false;
      }
    };

    /**
     * Returns a boolean indicating whether the given entry meets the criteria of the read mode.
     *
     * @param entry The entry, which may be null if it has been compacted from the log.
     * @param compactor The log compactor.
     * @return Indicates whether the given index/entry meets the criteria for reading using the read mode.
     */
    abstract boolean isValid(Indexed<? extends Entry<?>> entry, Compactor compactor);
  }

  /**
   * Returns the reader mode.
   *
   * @return The reader mode.
   */
  Mode mode();

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
  Indexed<? extends Entry<?>> currentEntry();

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
