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

import io.atomix.copycat.server.storage.util.OffsetPredicate;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

/**
 * Offset cleaner test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class OffsetPredicateTest {

  /**
   * Tests the offset predicate.
   */
  public void testOffsetPredicate() {
    OffsetPredicate cleaner = new OffsetPredicate();
    assertEquals(cleaner.count(), 0);
    cleaner.release(10);
    assertEquals(cleaner.count(), 1);
    assertTrue(cleaner.test(0L));
    assertFalse(cleaner.test(10L));
    assertTrue(cleaner.test(11L));
    cleaner.release(2048);
    assertEquals(cleaner.count(), 2);
    assertFalse(cleaner.test(2048L));
  }

}
