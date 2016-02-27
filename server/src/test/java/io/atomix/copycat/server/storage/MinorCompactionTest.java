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

import io.atomix.copycat.server.storage.compaction.Compaction;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

import static org.testng.Assert.*;

/**
 * Log compactor test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@Test
public class MinorCompactionTest extends AbstractLogTest {

  @Override
  protected Storage createStorage() {
    return tempStorageBuilder()
      .withMaxEntriesPerSegment(10)
      .build();
  }

  /**
   * Tests compacting the log.
   */
  public void testMinorCompaction() throws Throwable {
    writeEntries(31);

    assertEquals(log.length(), 31L);

    for (long index = 21; index < 28; index++) {
      log.release(index);
    }
    log.commit(31).compactor().minorIndex(31);

    CountDownLatch latch = new CountDownLatch(1);
    log.compactor().compact(Compaction.MINOR).thenRun(latch::countDown);
    latch.await();

    assertEquals(log.length(), 31L);

    for (long index = 21; index < 28; index++) {
      assertTrue(log.lastIndex() >= index);
      if (index % 2 != 0) {
        assertFalse(log.contains(index));
        try (TestEntry entry = log.get(index)) {
          assertNull(entry);
        }
      } else {
        assertTrue(log.contains(index));
        try (TestEntry entry = log.get(index)) {
          assertNotNull(entry);
        }
      }
    }
  }

  /**
   * Writes a set of session entries to the log.
   */
  private void writeEntries(int entries) {
    for (int i = 0; i < entries; i++) {
      try (TestEntry entry = log.create(TestEntry.class)) {
        entry.setTerm(1);
        entry.setPadding(1);
        if (entry.getIndex() % 2 == 0) {
          entry.setCompactionMode(Compaction.Mode.SEQUENTIAL);
        } else {
          entry.setCompactionMode(Compaction.Mode.QUORUM);
        }
        log.append(entry);
      }
    }
  }

}
