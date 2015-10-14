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

import org.testng.annotations.Factory;
import org.testng.annotations.Test;

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

//  /**
//   * Tests recovery of a closed log.
//   */
//  public void testRecoverAfterClose() {
//    appendEntries(1024);
//    assertEquals(log.length(), 1024);
//    log.close();
//
//    try (Log log = createLog()) {
//      assertEquals(log.length(), 1024);
//
//      for (long i = log.firstIndex(); i <= log.lastIndex(); i++) {
//        try (Entry entry = log.get(i)) {
//          assertEquals(entry.getIndex(), i);
//        }
//      }
//    }
//  }
//
//  /**
//   * Tests recovery of a log after compaction.
//   */
//  public void testRecoverAfterCompact() {
//    appendEntries(2048);
//    for (long i = 1; i <= 2048; i++) {
//      if (i % 3 == 0 || i % 3 == 1) {
//        log.clean(i);
//      }
//    }
//
//    for (long i = 1; i <= 2048; i++) {
//      if (i % 3 == 0 || i % 3 == 1) {
//        assertTrue(log.lastIndex() >= i);
//        assertTrue(log.contains(i));
//      }
//    }
//
//    log.commit(2048).compactor().compact().join();
//
//    try (Log log = createLog()) {
//      assertEquals(log.length(), 2048);
//      for (long i = 1; i <= 2048; i++) {
//        if (i % 3 == 0 || i % 3 == 1) {
//          assertTrue(log.lastIndex() >= i);
//          if (i != 1024) {
//            assertFalse(log.contains(i));
//            assertNull(log.get(i));
//          }
//        }
//      }
//    }
//  }

}
