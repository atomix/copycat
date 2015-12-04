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
import io.atomix.catalyst.serializer.ServiceLoaderTypeResolver;
import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.state.Member;
import io.atomix.copycat.server.storage.snapshot.Snapshot;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotStore;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import io.atomix.copycat.server.storage.system.Configuration;
import io.atomix.copycat.server.storage.system.MetaStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static org.testng.Assert.*;

/**
 * Snapshot store test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class SnapshotStoreTest {
  private String testId;

  /**
   * Returns a new snapshot store.
   */
  protected SnapshotStore createSnapshotStore() {
    return Storage.builder()
      .withSerializer(new Serializer(new ServiceLoaderTypeResolver()))
      .withDirectory(new File(String.format("target/test-logs/%s", testId)))
      .build()
      .openSnapshotStore("test");
  }

  /**
   * Tests writing a snapshot.
   */
  public void testWriteSnapshotChunks() {
    SnapshotStore store = createSnapshotStore();
    Snapshot snapshot = store.createSnapshot(1);
    assertEquals(snapshot.version(), 1);
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
    assertEquals(store.currentSnapshot().version(), 1);

    try (SnapshotReader reader = store.currentSnapshot().reader()) {
      assertEquals(reader.readLong(), 10);
      assertEquals(reader.readLong(), 11);
      assertEquals(reader.readLong(), 12);
    }
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
