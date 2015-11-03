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

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.ServiceLoaderTypeResolver;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;

import static org.testng.Assert.*;

/**
 * Log test.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 * @author Jonathan Halterman
 */
@Test
public abstract class LogTest extends AbstractLogTest {
  /**
   * Creates a new log.
   */
  @Override
  protected Log createLog() {
    return tempStorageBuilder().withDirectory(new File(String.format("target/test-logs/%s", logId)))
        .withMaxSegmentSize(Integer.MAX_VALUE)
        .withMaxEntriesPerSegment(entriesPerSegment)
        .withStorageLevel(storageLevel())
        .withSerializer(new Serializer(new ServiceLoaderTypeResolver()))
        .build()
        .open("copycat");
  }

  /**
   * Returns the log storage level.
   */
  protected abstract StorageLevel storageLevel();

  /**
   * Asserts that entries spanning 3 segments are appended with the expected indexes.
   */
  public void testAppend() {
    appendEntries(entriesPerSegment * 3);
    assertEquals(log.length(), entriesPerSegment * 3);
    assertEquals(log.segments.segments().size(), 3);
  }

  /**
   * Asserts that appending and getting entries works as expected across segments, with indexes set as expected.
   */
  public void testAppendAndGetEntries() {
    // Append 3 segments
    List<Long> indexes = appendEntries(entriesPerSegment * 3);
    assertFalse(log.isEmpty());
    assertFalse(log.containsEntry(0));

    // Assert that entries can be retrieved
    indexes.stream().forEach(i -> assertEquals(log.getEntry(i).getIndex(), i.longValue()));
    assertFalse(log.containsEntry(indexes.size() + 1));

    // Append 2 more segments
    List<Long> moreIndexes = appendEntries(entriesPerSegment * 2);
    moreIndexes.stream().forEach(i -> assertEquals(log.getEntry(i).getIndex(), i.longValue()));
    assertFalse(log.containsEntry(indexes.size() + moreIndexes.size() + 1));

    // Fetch 3 segments worth of entries, spanning 4 segments
    for (long index = 3; index <= entriesPerSegment * 3 + 2; index++) {
      assertEquals(log.getEntry(index).getIndex(), index);
    }
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void appendEntryShouldThrowWhenClosed() throws Exception {
    log.close();
    appendEntries(1);
  }

  /**
   * Asserts that {@link Log#cleanEntry(long)} works as expected across segments.
   */
  public void testClean() {
    appendEntries(entriesPerSegment * 3);
    for (int i = entriesPerSegment; i <= entriesPerSegment * 2 + 1; i++) {
      assertFalse(log.segments.segment(i).isClean(i));
      log.cleanEntry(i);
      assertTrue(log.segments.segment(i).isClean(i));
    }
  }

  /**
   * Asserts that {@link Log#cleanEntry(long)} prevents non-tombstone entries from being read.
   */
  public void testCleanGet() {
    appendEntries(entriesPerSegment * 3);
    for (int i = entriesPerSegment; i <= entriesPerSegment * 2 + 1; i++) {
      assertFalse(log.segments.segment(i).isClean(i));
      log.cleanEntry(i);
      assertNotNull(log.getEntry(i));
    }
    log.setGlobalIndex(entriesPerSegment * 2);
    for (int i = entriesPerSegment; i <= entriesPerSegment * 2; i++) {
      assertNull(log.getEntry(i));
    }
  }

  /**
   * Asserts that {@link Log#cleanEntry(long)} prevents tombstone entries from being read if the globalIndex is greater than the tombstone index.
   */
  public void testCleanGetTombstones() {
    appendEntries(entriesPerSegment * 3, true);
    for (int i = entriesPerSegment; i <= entriesPerSegment * 2 + 1; i++) {
      assertFalse(log.segments.segment(i).isClean(i));
      log.cleanEntry(i);
      assertNotNull(log.getEntry(i));
    }
    log.setGlobalIndex(entriesPerSegment * 2);
    for (int i = entriesPerSegment; i <= entriesPerSegment * 2; i++) {
      assertNull(log.getEntry(i));
    }
  }

  /**
   * Tests {@link Log#close()}
   */
  public void testClose() {
    appendEntries(5);
    assertTrue(log.isOpen());
    log.close();
    assertFalse(log.isOpen());
  }

  /**
   * Tests {@link Log#setCommitIndex(long)}
   */
  public void testCommit() {
    appendEntries(5);
    log.setCommitIndex(3);
    assertEquals(log.getCommitIndex(), 3);
  }

  /**
   * Asserts that {@link Log#containsEntry(long)} works as expected across segments and after compaction.
   */
  public void testContains() {
    assertFalse(log.containsEntry(0));
    assertFalse(log.containsEntry(1));

    List<Long> indexes = appendEntries(entriesPerSegment * 3);
    assertIndexes(indexes, 1, entriesPerSegment * 3);
    for (int i = 1; i <= entriesPerSegment * 3; i++)
      assertTrue(log.containsEntry(i));
    assertFalse(log.containsEntry(entriesPerSegment * 3 + 1));

    // Test after compaction
    log.setCommitIndex(entriesPerSegment * 3).setCompactIndex(entriesPerSegment * 3).setGlobalIndex(entriesPerSegment * 3);
    cleanAndCompact(entriesPerSegment + 1, entriesPerSegment * 2 + 1);
    assertTrue(log.containsEntry(entriesPerSegment));
    for (int i = entriesPerSegment + 1; i <= entriesPerSegment * 2; i++) {
      assertFalse(log.containsEntry(i));
    }
    if (log.length() > 3)
      assertTrue(log.containsEntry(entriesPerSegment * 2 + 2));
  }

  /**
   * Tests {@link Log#getFirstIndex()} across segments.
   */
  public void testFirstIndex() {
    assertEquals(log.getFirstIndex(), 0);
    appendEntries(entriesPerSegment * 3);
    assertEquals(log.getFirstIndex(), 1);

    // Asserts that firstIndex is unchanged after compaction
    log.setCommitIndex(entriesPerSegment * 3);
    cleanAndCompact(1, entriesPerSegment * 2 + 1);
    assertEquals(log.getFirstIndex(), 1);
  }

  /**
   * Tests {@link Log#getEntry(long)} across segments.
   */
  public void testGet() {
    appendEntries(entriesPerSegment * 3);
    for (int i = 1; i <= entriesPerSegment * 3; i++)
      assertEquals(log.getEntry(i).getIndex(), i);

    // Asserts get() after compaction
    log.setCommitIndex(entriesPerSegment * 3).setCompactIndex(entriesPerSegment * 3).setGlobalIndex(entriesPerSegment * 3);
    cleanAndCompact(entriesPerSegment + 1, entriesPerSegment * 2 + 1);
    assertCompacted(entriesPerSegment + 1, entriesPerSegment * 2);
  }

  /**
   * Tests {@link Log#isClosed()}.
   */
  public void testIsClosed() throws Throwable {
    assertFalse(log.isClosed());
    log.close();
    assertTrue(log.isClosed());
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testCloseShouldThrowWhenAlreadyClosed() throws Throwable {
    log.close();
    log.close();
  }

  /**
   * Tests {@link Log#isEmpty()}.
   */
  public void testIsEmpty() {
    assertTrue(log.isEmpty());
    appendEntries(1);
    assertFalse(log.isEmpty());
  }

  /**
   * Tests {@link Log#isOpen()}.
   */
  public void testIsOpen() throws Throwable {
    assertTrue(log.isOpen());
    log.close();
    assertFalse(log.isOpen());
  }

  /**
   * Tests {@link Log#getLastIndex()} across segments.
   */
  public void testLastIndex() {
    appendEntries(entriesPerSegment * 3);
    assertEquals(log.getLastIndex(), entriesPerSegment * 3);

    // Asserts that lastIndex is unchanged after compaction
    log.setCommitIndex(entriesPerSegment * 3);
    cleanAndCompact(entriesPerSegment + 1, entriesPerSegment * 2 + 1);
    assertEquals(log.getLastIndex(), entriesPerSegment * 3);
  }

  /**
   * Tests {@link Log#length()} across segments.
   */
  public void testLength() {
    assertEquals(log.length(), 0);

    appendEntries(entriesPerSegment * 3);
    assertEquals(log.segments.segments().size(), 3);
    assertEquals(log.length(), entriesPerSegment * 3);

    appendEntries(entriesPerSegment * 2);
    assertEquals(log.segments.segments().size(), 5);
    assertEquals(log.length(), entriesPerSegment * 5);

    // Asserts that length is unchanged after compaction
    log.setCommitIndex(entriesPerSegment * 5);
    cleanAndCompact(entriesPerSegment + 1, entriesPerSegment * 3);
    assertEquals(log.length(), entriesPerSegment * 5);
  }

  /**
   * Tests {@link Log#size()} across segments.
   */
  public void testSize() {
    assertEquals(log.size(), 64);

    appendEntries(entriesPerSegment * 3);
    assertEquals(log.segments.segments().size(), 3);
    assertEquals(log.size(), fullSegmentSize() * 3);
    assertFalse(log.isEmpty());

    appendEntries(entriesPerSegment * 2);
    assertEquals(log.segments.segments().size(), 5);
    assertEquals(log.size(), fullSegmentSize() * 5);

    log.setCommitIndex(entriesPerSegment * 5).setCompactIndex(entriesPerSegment * 5).setGlobalIndex(entriesPerSegment * 5);

    // Compact 2nd and 3rd segments
    cleanAndCompact(entriesPerSegment + 1, entriesPerSegment * 3);

    // Asserts that size() is changed after compaction
    assertEquals(log.size(), (entrySize() * entriesPerSegment * 3) + (log.segments.segments().size() * SegmentDescriptor.BYTES));
  }

  /**
   * Tests skipping entries in the log across segments.
   */
  public void testSkip() throws Throwable {
    appendEntries(100);

    log.skip(10);
    assertEquals(log.getLastIndex(), 110);

    long index = appendEntries(1).get(0);
    assertEquals(log.length(), 111);

    log.setCommitIndex(111);
    try (TestEntry entry = log.getEntry(101)) {
      assertNull(entry);
    }

    try (TestEntry entry = log.getEntry(index)) {
      assertEquals(entry.getTerm(), 1);
    }
  }

  /**
   * Tests {@link Log#truncate(long)}.
   */
  public void testTruncate() throws Throwable {
    appendEntries(100);
    assertEquals(log.getLastIndex(), 100);
    log.truncate(10);
    assertEquals(log.getLastIndex(), 10);
    appendEntries(10);
    assertEquals(log.getLastIndex(), 20);
  }

  /**
   * Tests truncating and then appending entries in the log.
   */
  public void testTruncateAppend() throws Throwable {
    appendEntries(10);
    assertEquals(log.getLastIndex(), 10);
    TestEntry entry = log.createEntry(TestEntry.class).setIndex(10).setTerm(2);
    log.truncate(entry.getIndex() - 1).appendEntry(entry);
    TestEntry result89 = log.getEntry(9);
    assertEquals(result89.getTerm(), 1);
    TestEntry result90 = log.getEntry(10);
    assertEquals(result90.getTerm(), 2);
  }

  /**
   * Tests truncating and then appending entries in the log.
   */
  public void testTruncateUncommitted() throws Throwable {
    appendEntries(10);
    log.setCommitIndex(1);
    assertEquals(log.getLastIndex(), 10);
    TestEntry entry = log.createEntry(TestEntry.class).setIndex(10).setTerm(2);
    log.truncate(entry.getIndex() - 1).appendEntry(entry);
    TestEntry result89 = log.getEntry(9);
    assertEquals(result89.getTerm(), 1);
    TestEntry result90 = log.getEntry(10);
    assertEquals(result90.getTerm(), 2);
  }

  /**
   * Tests truncating to a skipped index.
   */
  public void testTruncateSkipped() throws Throwable {
    appendEntries(100);
    assertEquals(log.getLastIndex(), 100);
    log.skip(10);
    appendEntries(100);
    assertEquals(log.getLastIndex(), 210);
    log.truncate(105);
    assertEquals(log.getLastIndex(), 105);
    assertNull(log.setCommitIndex(105).getEntry(105));
  }

  /**
   * Tests emptying the log.
   */
  public void testTruncateZero() throws Throwable {
    appendEntries(100);
    assertEquals(log.getFirstIndex(), 1);
    assertEquals(log.getLastIndex(), 100);
    log.truncate(0);
    assertEquals(log.getFirstIndex(), 0);
    assertEquals(log.getLastIndex(), 0);
    appendEntries(10);
    assertEquals(log.getFirstIndex(), 1);
    assertEquals(log.getLastIndex(), 10);
  }
}
