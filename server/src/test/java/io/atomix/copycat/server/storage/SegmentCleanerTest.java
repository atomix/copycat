/*
 * Copyright 2016 the original author or authors.
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

import io.atomix.copycat.server.storage.compaction.Compaction;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Segment cleaner test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class SegmentCleanerTest {

  /**
   * Tests the segment cleaner.
   */
  public void testCleaner() throws Exception {
    SegmentCleaner cleaner = new SegmentCleaner();

    // 111001
    cleaner.set(0, Compaction.Mode.QUORUM); // 01
    cleaner.set(1, Compaction.Mode.SEQUENTIAL); // 10
    cleaner.set(3, Compaction.Mode.SNAPSHOT); // 11

    assertEquals(cleaner.get(0), Compaction.Mode.QUORUM); // 01
    assertEquals(cleaner.get(1), Compaction.Mode.SEQUENTIAL); // 10
    assertEquals(cleaner.get(2), Compaction.Mode.NONE);
    assertEquals(cleaner.get(3), Compaction.Mode.SNAPSHOT); // 11
  }

}
