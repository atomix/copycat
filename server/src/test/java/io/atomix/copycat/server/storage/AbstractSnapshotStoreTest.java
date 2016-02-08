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
 * limitations under the License
 */
package io.atomix.copycat.server.storage;

import io.atomix.copycat.server.storage.snapshot.Snapshot;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotStore;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Snapshot store test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public abstract class AbstractSnapshotStoreTest {

  /**
   * Returns a new snapshot store.
   */
  protected abstract SnapshotStore createSnapshotStore();

  /**
   * Tests writing a snapshot.
   */
  public void testWriteSnapshotChunks() {
    SnapshotStore store = createSnapshotStore();
    Snapshot snapshot = store.createSnapshot(1);
    assertEquals(snapshot.index(), 1);
    assertNotEquals(snapshot.timestamp(), 0);
    assertNull(store.currentSnapshot());

    try (SnapshotWriter writer = snapshot.writer()) {
      writer.writeLong(10);
    }

    assertNull(store.currentSnapshot());

    try (SnapshotWriter writer = snapshot.writer()) {
      writer.writeLong(11);
    }

    assertNull(store.currentSnapshot());

    try (SnapshotWriter writer = snapshot.writer()) {
      writer.writeLong(12);
    }

    assertNull(store.currentSnapshot());
    snapshot.complete();
    assertNotNull(store.currentSnapshot());
    assertEquals(store.currentSnapshot().index(), 1);

    try (SnapshotReader reader = store.currentSnapshot().reader()) {
      assertEquals(reader.readLong(), 10);
      assertEquals(reader.readLong(), 11);
      assertEquals(reader.readLong(), 12);
    }
  }

}
