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

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.server.storage.snapshot.Snapshot;
import io.atomix.copycat.server.storage.snapshot.SnapshotStore;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * File snapshot store test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class FileSnapshotStoreTest extends AbstractSnapshotStoreTest {
  private String testId;

  /**
   * Returns a new snapshot store.
   */
  protected SnapshotStore createSnapshotStore() {
    Storage storage = Storage.builder()
      .withStorageLevel(StorageLevel.DISK)
      .withDirectory(new File(String.format("target/test-logs/%s", testId)))
      .build();
    return new SnapshotStore("test", storage, new Serializer());
  }

  /**
   * Tests storing and loading snapshots.
   */
  public void testStoreLoadSnapshot() {
    SnapshotStore store = createSnapshotStore();

    Snapshot snapshot = store.createSnapshot(1);
    try (SnapshotWriter writer = snapshot.writer()) {
      writer.writeLong(10);
    }
    snapshot.complete();
    assertNotNull(store.currentSnapshot());
    store.close();

    store = createSnapshotStore();
    assertNotNull(store.currentSnapshot());
    assertEquals(store.currentSnapshot().index(), 1);
  }

  @BeforeMethod
  @AfterMethod
  protected void cleanupStorage() throws IOException {
    Path directory = Paths.get("target/test-logs/");
    if (Files.exists(directory)) {
      Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    }
    testId = UUID.randomUUID().toString();
  }

}
