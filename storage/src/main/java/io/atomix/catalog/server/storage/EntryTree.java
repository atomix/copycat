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
  private final Map<Long, Entry> tombstones = new ConcurrentHashMap<>();
  private final Map<Long, Entry> entries = new ConcurrentHashMap<>();
  private final Map<Long, Long> trees = new ConcurrentHashMap<>();

  /**
   * Adds an entry to the tree.
   * <p>
   * When an entry is added to the tree, if the entry is a tombstone and its index is greater than the previous
   * tombstone, store the tombstone in memory.
   *
   * @param entry The entry to add.
   * @return The entry tree.
   */
  EntryTree add(Entry entry) {
    if (entry.isTombstone()) {
      Entry tombstone = tombstones.get(entry.getAddress());
      if (tombstone != null) {
        if (entry.getIndex() > tombstone.getIndex()) {
          entry.acquire();
          tombstones.put(entry.getAddress(), entry);
          update(tombstone.getAddress(), tombstone.getId());
          tombstone.release();
        } else {
          update(entry.getAddress(), entry.getId());
        }
      } else {
        entry.acquire();
        tombstones.put(entry.getAddress(), entry);
      }
    } else {
      Entry tombstone = tombstones.remove(entry.getAddress());
      if (tombstone != null)
        tombstone.release();
      update(entry.getAddress(), entry.getId());
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
    Entry tombstone = tombstones.get(entry.getAddress());
    if (tombstone == null) {
      return true;
    } else if (tombstone.getIndex() == entry.getIndex()) {
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
    Entry tombstone = tombstones.get(entry.getAddress());
    if (tombstone != null) {
      if (tombstone.getIndex() == entry.getIndex()) {
        tombstones.remove(entry.getAddress());
        trees.remove(entry.getAddress());
        tombstone.release();
      } else {
        update(entry.getAddress(), entry.getId());
      }
    } else {
      update(entry.getAddress(), entry.getId());
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
