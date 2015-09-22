/*
 * Copyright 2015 the original author or authors.
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
 * limitations under the License.
 */
package io.atomix.catalog.server.storage;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Entry tree.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class EntryTree {
  private final Map<Long, Entry> entries = new ConcurrentHashMap<>();
  private final Map<Long, Long> trees = new ConcurrentHashMap<>();

  /**
   * Adds an entry to the tree.
   *
   * @param entry The entry to add.
   * @return The entry tree.
   */
  EntryTree add(Entry entry) {
    Entry previous = entries.get(entry.getAddress());
    if (previous != null) {
      if (previous.getIndex() < entry.getIndex()) {
        entry.acquire();
        entries.put(entry.getAddress(), entry);
        update(previous.getAddress(), previous.getId());
        previous.release();
      } else {
        update(entry.getAddress(), entry.getId());
      }
    } else {
      entry.acquire();
      entries.put(entry.getAddress(), entry);
    }
    return this;
  }

  /**
   * Returns a boolean value indicating whether the given entry can be deleted.
   *
   * @param entry The entry to check.
   * @return Indicates whether the given entry can be deleted.
   */
  boolean canDelete(Entry entry) {
    Entry previous = entries.get(entry.getAddress());
    if (previous == null)
      return true;

    if (previous.getIndex() == entry.getIndex()) {
      Long tree = trees.get(entry.getAddress());
      return tree == null || tree == 0;
    }
    return true;
  }

  /**
   * Deletes an entry from the tree.
   *
   * @param entry The entry to delete.
   * @return The entry tree.
   */
  EntryTree delete(Entry entry) {
    Entry previous = entries.get(entry.getAddress());
    if (previous != null) {
      if (previous.getIndex() == entry.getIndex()) {
        entries.remove(entry.getAddress());
        trees.remove(entry.getAddress());
        previous.release();
      } else {
        update(entry.getAddress(), entry.getId());
      }
    }
    return this;
  }

  /**
   * Updates the tree for the given address and entry.
   *
   * @param address The address to update.
   * @param entry   The entry to update.
   */
  private boolean update(long address, long entry) {
    Long tree = trees.get(address);
    if (tree == null) {
      tree = 0L;
    }
    tree ^= entry;
    trees.put(address, tree);
    return tree == 0;
  }

}
