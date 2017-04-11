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
package io.atomix.copycat.server.storage;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.DirectBuffer;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.server.storage.compaction.Compaction;
import io.atomix.copycat.server.storage.util.StorageSerialization;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * Abstract log test.
 * 
 * @author Jonathan Halterman
 */
@Test
public abstract class AbstractLogTest {
  protected int entryPadding;
  protected int entriesPerSegment = 3;
  protected Storage storage;
  protected Log log;
  String logId;

  protected abstract Storage createStorage();

  /**
   * Creates a new test log.
   */
  protected Log createLog() {
    return new Log(logId, storage, new Serializer().resolve(new StorageSerialization()).register(TestEntry.class));
  }

  /**
   * Returns a set of tests to run for varied log configurations.
   */
  protected Object[] testsFor(Class<? extends LogTest> testClass) throws Throwable {
    List<Object> tests = new ArrayList<>();
    for (int i = 1; i < 10; i++) {
      LogTest test = testClass.newInstance();
      test.entriesPerSegment = i;
      test.entryPadding = i / 3;
      tests.add(test);
    }

    return tests.toArray(new Object[tests.size()]);
  }

  protected int entrySize() {
    Serializer serializer = new Serializer();
    serializer.register(TestEntry.class);
    Buffer buffer = DirectBuffer.allocate(1000);
    TestEntry entry = new TestEntry();
    entry.setPadding(entryPadding);
    serializer.writeObject(entry, buffer);
    return (int) buffer.position() + Short.BYTES + Long.BYTES + Byte.BYTES;
  }

  @BeforeMethod
  void setLog() throws Exception {
    logId = UUID.randomUUID().toString();
    storage = createStorage();
    log = new Log(logId, storage, new Serializer().resolve(new StorageSerialization()));
    log.serializer().register(TestEntry.class);
    assertTrue(log.isOpen());
    assertFalse(log.isClosed());
    assertTrue(log.isEmpty());
  }

  @AfterMethod
  protected void deleteLog() {
    try {
      if (log.isOpen())
        log.close();
    } catch (Exception ignore) {
    } finally {
      assertFalse(log.isOpen());
      assertTrue(log.isClosed());
      storage.deleteLog(logId);
    }
  }

  /**
   * Returns the size of a full segment given the entrySize, entriesPerSegment, and SegmentDescriptor.
   */
  int fullSegmentSize() {
    return entriesPerSegment * entrySize() + SegmentDescriptor.BYTES;
  }

  protected Storage.Builder tempStorageBuilder() {
    return Storage.builder().withDirectory(new File(String.format("target/test-logs/%s", logId)));
  }

  @BeforeTest
  @AfterTest
  protected void cleanLogDir() throws IOException {
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
  }

  /**
   * Appends {@code numEntries} increasingly numbered ByteBuffer wrapped entries to the log.
   */
  protected List<Long> appendEntries(int numEntries) {
    return appendEntries(numEntries, Compaction.Mode.QUORUM);
  }

  /**
   * Appends {@code numEntries} increasingly numbered ByteBuffer wrapped entries to the log.
   */
  protected List<Long> appendEntries(int numEntries, Compaction.Mode mode) {
    List<Long> indexes = new ArrayList<>(numEntries);
    for (int i = 0; i < numEntries; i++) {
      try (TestEntry entry = log.create(TestEntry.class)) {
        entry.setTerm(1).setCompactionMode(mode).setPadding(entryPadding);
        indexes.add(log.append(entry));
      }
    }
    return indexes;
  }

  protected static void assertIndexes(List<Long> indexes, int start, int end) {
    for (int i = 0, j = start; j <= end; i++, j++)
      assertEquals(indexes.get(i).longValue(), j);
  }

  protected void cleanAndCompact(int startIndex, int endIndex) {
    for (int i = startIndex; i <= endIndex; i++) {
      log.release(i);
    }

    log.compactor().compact(Compaction.MAJOR).join();
  }

  protected void assertCompacted(int startIndex, int endIndex) {
    for (int i = startIndex; i <= endIndex; i++) {
      assertNull(log.get(i));
    }
  }
  
  protected void printLog() {
    for (int i = 1; i < log.length(); i++) {
      System.out.println(log.get(i).toString());
    }
  }
}
