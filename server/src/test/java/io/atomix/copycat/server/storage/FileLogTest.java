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

import io.atomix.copycat.server.storage.entry.Entry;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * File log test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class FileLogTest extends LogTest {

  @Factory
  public Object[] createTests() throws Throwable {
    return testsFor(FileLogTest.class);
  }

  @Override
  protected StorageLevel storageLevel() {
    return StorageLevel.DISK;
  }

  /**
   * Tests recovery of a closed log.
   */
  public void testRecoverAfterClose() {
    appendEntries(entriesPerSegment * 5);
    assertEquals(log.length(), entriesPerSegment * 5);
    log.close();

    try (Log log = createLog()) {
      assertEquals(log.length(), entriesPerSegment * 5);

      for (long i = log.firstIndex(); i <= log.lastIndex(); i++) {
        try (Entry entry = log.get(i)) {
          assertEquals(entry.getIndex(), i);
        }
      }
    }
  }

  /**
   * Tests recovery of a log after compaction.
   */
  public void testRecoverAfterCompact() {
    appendEntries(entriesPerSegment * 5);
    for (long i = 1; i <= entriesPerSegment * 5; i++) {
      if (i % 3 == 0 || i % 3 == 1) {
        log.release(i);
      }
    }

    for (long i = 1; i <= entriesPerSegment * 5; i++) {
      if (i % 3 == 0 || i % 3 == 1) {
        assertTrue(log.lastIndex() >= i);
        assertTrue(log.contains(i));
      }
    }

    log.commit(entriesPerSegment * 5).compactor().minorIndex(entriesPerSegment * 5).compact().join();
    log.close();

    try (Log log = createLog()) {
      assertEquals(log.length(), entriesPerSegment * 5);

      for (long i = 1; i <= entriesPerSegment * 5; i++) {
        if (i % 3 == 0 || i % 3 == 1) {
          assertTrue(log.lastIndex() >= i);
          if (i <= entriesPerSegment * 4) {
            assertFalse(log.contains(i));
            assertNull(log.get(i));
          }
        }
      }
    }
  }

  /**
   * Tests recovering from an inconsistent disk.
   */
  public void testRecoverInconsistentDisk() throws Throwable {
    appendEntries(entriesPerSegment * 2);
    Segment firstSegment = log.segments.firstSegment();
    log.segments.createSegment(SegmentDescriptor.builder()
      .withId(firstSegment.descriptor().id())
      .withIndex(firstSegment.descriptor().index())
      .withVersion(firstSegment.descriptor().version() + 1)
      .withMaxSegmentSize(firstSegment.descriptor().maxSegmentSize())
      .withMaxEntries(firstSegment.descriptor().maxEntries())
      .build()).close();

    log.close();

    try (Log log = createLog()) {
      assertEquals(log.length(), entriesPerSegment * 2);
      assertEquals(log.segments.firstSegment().descriptor().version(), 1);
    }
  }

}
