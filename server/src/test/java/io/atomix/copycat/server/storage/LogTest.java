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
import io.atomix.copycat.server.storage.compaction.Compaction;
import io.atomix.copycat.server.storage.entry.Entry;

import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
        .withMaxSegmentSize(maxSegmentSize)
        .withMaxEntriesPerSegment(maxEntriesPerSegment)
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
    for (int i = 1; i <= entriesPerSegment * 3; i++) {
      try (TestEntry entry = log.create(TestEntry.class)) {
        assertEquals(log.append(entry), i);
      }
    }

    assertEquals(log.segments.segments().size(), 3);
  }

  /**
   * Asserts that appending and getting entries works as expected across segments, with indexes set as expected.
   */
  public void testAppendAndGetEntries() {
    // Append 3 segments
    List<Long> indexes = appendEntries(entriesPerSegment * 3);
    assertFalse(log.isEmpty());
    assertFalse(log.contains(0));

    // Assert that entries can be retrieved
    indexes.stream().forEach(i -> assertEquals(log.get(i).getIndex(), i.longValue()));
    assertFalse(log.contains(indexes.size() + 1));

    // Append 2 more segments
    List<Long> moreIndexes = appendEntries(entriesPerSegment * 2);
    moreIndexes.stream().forEach(i -> assertEquals(log.get(i).getIndex(), i.longValue()));
    assertFalse(log.contains(indexes.size() + moreIndexes.size() + 1));

    // Fetch 3 segments worth of entries, spanning 4 segments
    for (long index = 3; index <= entriesPerSegment * 3 + 2; index++) {
      assertEquals(log.get(index).getIndex(), index);
    }
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void appendEntryShouldThrowWhenClosed() throws Exception {
    log.close();
    appendEntries(1);
  }

  /**
   * Asserts that {@link Log#clean(long)} works as expected across segments.
   */
  public void testClean() {
    appendEntries(entriesPerSegment * 3);
    for (int i = 3; i <= entriesPerSegment * 2 + 2; i++) {
      assertFalse(log.segments.segment(i).isClean(i));
      log.clean(i);
      assertTrue(log.segments.segment(i).isClean(i));
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
   * Tests {@link Log#commit(long)}
   */
  public void testCommit() {
    appendEntries(5);
    log.commit(3);
    assertEquals(log.segments.commitIndex(), 3);
  }

  /**
   * Asserts that {@link Log#contains(long)} works as expected across segments and after compaction.
   */
  public void testContains() {
    assertFalse(log.contains(0));
    assertFalse(log.contains(1));

    List<Long> indexes = appendEntries(entriesPerSegment * 3);
    assertIndexes(indexes, 1, entriesPerSegment * 3);
    for (int i = 1; i <= entriesPerSegment * 3; i++)
      assertTrue(log.contains(i));
    assertFalse(log.contains(entriesPerSegment * 3 + 1));
    
    log.commit(entriesPerSegment * 3);

    for (long i = log.firstIndex(); i <= log.lastIndex(); i++) {
      try (Entry entry = log.get(i)) {
        System.out.println(entry.getIndex());
      }
    }

    for (int i = 3; i <= entriesPerSegment + 2; i++) {
      log.clean(i);
      System.out.println("Cleaning " + i);
    }

    log.compactor().compact(Compaction.MAJOR).join();

    for (long i = log.firstIndex(); i <= log.lastIndex(); i++) {
      try (Entry entry = log.get(i)) {
        System.out.println(entry != null ? entry.getIndex() : null);
      }
    }
  }

  /**
   * Tests {@link Log#firstIndex()} across segments.
   */
  public void testFirstIndex() {
    appendEntries(entriesPerSegment * 3);
    assertEquals(log.firstIndex(), 1);

    // TODO check after compaction
  }

  /**
   * Tests {@link Log#get(long)} across segments.
   */
  public void testGet() {
    appendEntries(entriesPerSegment * 3);
    for (int i = 1; i <= entriesPerSegment * 3; i++)
      assertEquals(log.get(i).getIndex(), i);
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
   * Tests {@link Log#lastIndex()} across segments.
   */
  public void testLastIndex() {
    appendEntries(entriesPerSegment * 3);
    assertEquals(log.lastIndex(), entriesPerSegment * 3);

    // TODO test after compaction
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

    // TODO compact and assert
    // log.removeAfter(entriesPerSegment * 2 + 1);
    // assertEquals(log.segments.count(), 3);
    // assertEquals(log.length(), (entriesPerSegment * 2 + 1));
  }

  /**
   * Tests skipping entries in the log across segments.
   */
  public void testSkip() throws Throwable {
    appendEntries(100);

    log.skip(10);
    assertEquals(log.lastIndex(), 110);

    long index = appendEntries(1).get(0);
    assertEquals(log.length(), 111);

    log.commit(111);
    try (TestEntry entry = log.get(101)) {
      assertNull(entry);
    }

    try (TestEntry entry = log.get(index)) {
      assertEquals(entry.getTerm(), 1);
    }
  }

  /**
   * Tests {@link Log#truncate(long)}.
   */
  public void testTruncate() throws Throwable {
    appendEntries(100);
    assertEquals(log.lastIndex(), 100);
    log.truncate(10);
    assertEquals(log.lastIndex(), 10);
    appendEntries(10);
    assertEquals(log.lastIndex(), 20);
  }

  /**
   * Tests truncating and then appending entries in the log.
   */
  public void testTruncateAppend() throws Throwable {
    appendEntries(10);
    assertEquals(log.lastIndex(), 10);
    TestEntry entry = log.create(TestEntry.class).setIndex(10).setTerm(2);
    log.truncate(entry.getIndex() - 1).append(entry);
    TestEntry result89 = log.get(9);
    assertEquals(result89.getTerm(), 1);
    TestEntry result90 = log.get(10);
    assertEquals(result90.getTerm(), 2);
  }

  /**
   * Tests truncating and then appending entries in the log.
   */
  public void testTruncateUncommitted() throws Throwable {
    appendEntries(10);
    log.commit(1);
    assertEquals(log.lastIndex(), 10);
    TestEntry entry = log.create(TestEntry.class).setIndex(10).setTerm(2);
    log.truncate(entry.getIndex() - 1).append(entry);
    TestEntry result89 = log.get(9);
    assertEquals(result89.getTerm(), 1);
    TestEntry result90 = log.get(10);
    assertEquals(result90.getTerm(), 2);
  }

  /**
   * Tests truncating to a skipped index.
   */
  public void testTruncateSkipped() throws Throwable {
    appendEntries(100);
    assertEquals(log.lastIndex(), 100);
    log.skip(10);
    appendEntries(100);
    assertEquals(log.lastIndex(), 210);
    log.truncate(105);
    assertEquals(log.lastIndex(), 105);
    assertNull(log.commit(105).get(105));
  }

  /**
   * Tests emptying the log.
   */
  public void testTruncateZero() throws Throwable {
    appendEntries(100);
    assertEquals(log.lastIndex(), 100);
    log.truncate(0);
    assertEquals(log.lastIndex(), 0);
    appendEntries(10);
    assertEquals(log.lastIndex(), 10);
  }

  /**
   * Tests {@link Log#size()} across segments.
   */
  public void testSize() {
    assertEquals(log.size(), 64);

    appendEntries(entriesPerSegment * 3);
    assertEquals(log.segments.segments().size(), 3);
    assertEquals(log.size(), maxUsableSegmentSpace * 3);
    assertFalse(log.isEmpty());

    appendEntries(entriesPerSegment * 2);
    assertEquals(log.segments.segments().size(), 5);
    assertEquals(log.size(), maxUsableSegmentSpace * 5);

    // TODO compact and assert
    // log.removeAfter(entriesPerSegment * 2 + 1);
    // assertEquals(log.segments().size(), 3);
    // assertEquals(log.size(), entrySize * (entriesPerSegment * 2 + 1));
  }

  /**
   * Appends {@code numEntries} increasingly numbered ByteBuffer wrapped entries to the log.
   */
  protected List<Long> appendEntries(int numEntries) {
    return appendEntries(numEntries, (int) log.length() + 1);
  }

  /**
   * Appends {@code numEntries} increasingly numbered ByteBuffer wrapped entries to the log, starting at the
   * {@code startingId}.
   */
  protected List<Long> appendEntries(int numEntries, int startingId) {
    List<Integer> entryIds = IntStream.range(startingId, startingId + numEntries).boxed().collect(Collectors.toList());
    return entryIds.stream().map(entryId -> {
      try (TestEntry entry = log.create(TestEntry.class)) {
        entry.setTerm(1);
        return log.append(entry);
      }
    }).collect(Collectors.toList());
  }

  protected static void assertIndexes(List<Long> indexes, int start, int end) {
    for (int i = 0, j = start; j <= end; i++, j++)
      assertEquals(indexes.get(i).longValue(), j);
  }
}
