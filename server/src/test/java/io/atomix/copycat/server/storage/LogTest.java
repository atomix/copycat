/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.copycat.server.storage;

import io.atomix.copycat.server.storage.entry.CloseSessionEntry;
import io.atomix.copycat.server.storage.entry.OpenSessionEntry;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Log test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
@SuppressWarnings("unchecked")
public class LogTest {

  private Log createLog() {
    return Storage.builder()
      .withStorageLevel(StorageLevel.MEMORY)
      .build()
      .openLog("test");
  }

  public void testLogWriteRead() throws Exception {
    Log log = createLog();
    LogWriter writer = log.writer();
    LogReader reader = log.createReader(1, Reader.Mode.ALL);

    // Append a couple entries.
    Indexed indexed;
    assertEquals(writer.nextIndex(), 1);
    indexed = writer.append(1, new OpenSessionEntry(System.currentTimeMillis(), "test1", "test", 1000));
    assertEquals(indexed.index(), 1);
    assertEquals(indexed.term(), 1);

    assertEquals(writer.nextIndex(), 2);
    indexed = writer.append(new Indexed<>(2, 1, new CloseSessionEntry(System.currentTimeMillis(), 1), 0));
    assertEquals(indexed.index(), 2);
    assertEquals(indexed.term(), 1);

    // Test reading the register entry.
    Indexed<OpenSessionEntry> openSession;
    assertTrue(reader.hasNext());
    openSession = (Indexed<OpenSessionEntry>) reader.next();
    assertEquals(openSession.index(), 1);
    assertEquals(openSession.term(), 1);
    assertEquals(openSession.entry().name(), "test1");
    assertEquals(openSession.entry().typeName(), "test");
    assertEquals(openSession.entry().timeout(), 1000);
    assertEquals(reader.currentEntry(), openSession);
    assertEquals(reader.currentIndex(), 1);

    // Test reading the unregister entry.
    Indexed<CloseSessionEntry> closeSession;
    assertTrue(reader.hasNext());
    assertEquals(reader.nextIndex(), 2);
    closeSession = (Indexed<CloseSessionEntry>) reader.next();
    assertEquals(closeSession.index(), 2);
    assertEquals(closeSession.term(), 1);
    assertEquals(closeSession.entry().session(), 1);
    assertEquals(reader.currentEntry(), closeSession);
    assertEquals(reader.currentIndex(), 2);
    assertFalse(reader.hasNext());

    // Test opening a new reader and reading from the log.
    reader = log.createReader(1, Reader.Mode.ALL);
    assertTrue(reader.hasNext());
    openSession = (Indexed<OpenSessionEntry>) reader.next();
    assertEquals(openSession.index(), 1);
    assertEquals(openSession.term(), 1);
    assertEquals(openSession.entry().name(), "test1");
    assertEquals(openSession.entry().typeName(), "test");
    assertEquals(openSession.entry().timeout(), 1000);
    assertEquals(reader.currentEntry(), openSession);
    assertEquals(reader.currentIndex(), 1);
    assertTrue(reader.hasNext());

    assertTrue(reader.hasNext());
    assertEquals(reader.nextIndex(), 2);
    closeSession = (Indexed<CloseSessionEntry>) reader.next();
    assertEquals(closeSession.index(), 2);
    assertEquals(closeSession.term(), 1);
    assertEquals(closeSession.entry().session(), 1);
    assertEquals(reader.currentEntry(), closeSession);
    assertEquals(reader.currentIndex(), 2);
    assertFalse(reader.hasNext());

    // Reset the reader.
    reader.reset();

    // Test opening a new reader and reading from the log.
    reader = log.createReader(1, Reader.Mode.ALL);
    assertTrue(reader.hasNext());
    openSession = (Indexed<OpenSessionEntry>) reader.next();
    assertEquals(openSession.index(), 1);
    assertEquals(openSession.term(), 1);
    assertEquals(openSession.entry().name(), "test1");
    assertEquals(openSession.entry().typeName(), "test");
    assertEquals(openSession.entry().timeout(), 1000);
    assertEquals(reader.currentEntry(), openSession);
    assertEquals(reader.currentIndex(), 1);
    assertTrue(reader.hasNext());

    assertTrue(reader.hasNext());
    assertEquals(reader.nextIndex(), 2);
    closeSession = (Indexed<CloseSessionEntry>) reader.next();
    assertEquals(closeSession.index(), 2);
    assertEquals(closeSession.term(), 1);
    assertEquals(closeSession.entry().session(), 1);
    assertEquals(reader.currentEntry(), closeSession);
    assertEquals(reader.currentIndex(), 2);
    assertFalse(reader.hasNext());

    // Truncate the log and write a different entry.
    writer.truncate(1);
    assertEquals(writer.nextIndex(), 2);
    indexed = writer.append(new Indexed<>(2, 2, new CloseSessionEntry(System.currentTimeMillis(), 1), 0));
    assertEquals(indexed.index(), 2);
    assertEquals(indexed.term(), 2);

    // Reset the reader to a specific index and read the last entry again.
    reader.reset(1);

    assertTrue(reader.hasNext());
    assertEquals(reader.nextIndex(), 2);
    closeSession = (Indexed<CloseSessionEntry>) reader.next();
    assertEquals(closeSession.index(), 2);
    assertEquals(closeSession.term(), 2);
    assertEquals(closeSession.entry().session(), 1);
    assertEquals(reader.currentEntry(), closeSession);
    assertEquals(reader.currentIndex(), 2);
    assertFalse(reader.hasNext());
  }
}