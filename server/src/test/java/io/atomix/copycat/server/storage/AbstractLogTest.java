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
import java.util.UUID;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.DirectBuffer;
import io.atomix.catalyst.serializer.Serializer;
import static org.testng.Assert.*;

/**
 * Abstract log test.
 * 
 * @author Jonathan Halterman
 */
@Test
public abstract class AbstractLogTest {
  protected int maxSegmentSize = 100;
  protected int maxUsableSegmentSize = maxSegmentSize - SegmentDescriptor.BYTES;
  protected int maxEntriesPerSegment = 100;
  protected int entrySize = entrySize();
  protected int entriesPerSegment = (int) Math.ceil((double) maxUsableSegmentSize / (double) entrySize);
  protected int maxUsableSegmentSpace = ((entrySize * entriesPerSegment) + SegmentDescriptor.BYTES);
  protected Log log;
  String logId;

  protected abstract Log createLog();

  private int entrySize() {
    Serializer serializer = new Serializer();
    serializer.register(TestEntry.class);
    Buffer buffer = DirectBuffer.allocate(1000);
    serializer.writeObject(new TestEntry(), buffer);
    return (int) buffer.position() + Short.BYTES + Long.BYTES;
  }

  @BeforeMethod
  void setLog() throws Exception {
    logId = UUID.randomUUID().toString();
    log = createLog();
    assertTrue(log.isOpen());
    assertFalse(log.isClosed());
    assertTrue(log.isEmpty());
  }

  @AfterMethod
  protected void deleteLog() {
    try {
      log.close();
    } catch (Exception ignore) {
    } finally {
      assertFalse(log.isOpen());
      assertTrue(log.isClosed());
      log.delete();
    }
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

}
