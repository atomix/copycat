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
package io.atomix.catalogue.server.storage.cleaner;

import io.atomix.catalogue.server.storage.entry.Entry;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Entry tree.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class EntryTree {
  private final Map<Entry, Entry> tombstones = new ConcurrentHashMap<>();
  private final Map<Entry, Long> trees = new ConcurrentHashMap<>();

  /**
   * Adds an entry to the tree.
   * <p>
   * When an entry is added to the tree, if the entry is a tombstone and its index is greater than the previous
   * tombstone, store the tombstone in memory.
   *
   * @param entry The entry to add.
   * @return The entry tree.
   */
  public EntryTree add(Entry entry, boolean isTombstone) {
    if (isTombstone) {
      Entry tombstone = tombstones.get(entry);
      if (tombstone != null) {
        if (entry.getIndex() > tombstone.getIndex()) {
          tombstones.remove(tombstone);
          entry.acquire();
          tombstones.put(entry, entry);
          update(tombstone);
          tombstone.release();
        } else {
          update(entry);
        }
      } else {
        entry.acquire();
        tombstones.put(entry, entry);
      }
    } else {
      Entry tombstone = tombstones.remove(entry);
      if (tombstone != null)
        tombstone.release();
      update(entry);
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
    Entry tombstone = tombstones.get(entry);
    if (tombstone == null) {
      return true;
    } else if (tombstone.getIndex() == entry.getIndex()) {
      Long tree = trees.get(entry);
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
    Entry tombstone = tombstones.get(entry);
    if (tombstone != null) {
      if (tombstone.getIndex() == entry.getIndex()) {
        tombstones.remove(entry);
        trees.remove(entry);
        tombstone.release();
      } else {
        update(entry);
      }
    } else {
      update(entry);
    }
    return this;
  }

  /**
   * Updates the tree for the given address and entry.
   *
   * @param entry The entry to update.
   */
  private boolean update(Entry entry) {
    Long tree = trees.get(entry);
    if (tree == null) {
      tree = 0L;
    }
    tree ^= entry.getId();
    trees.put(entry, tree);
    return tree == 0;
  }

}
