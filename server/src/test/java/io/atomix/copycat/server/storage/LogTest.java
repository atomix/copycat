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

import io.atomix.copycat.server.storage.entry.RegisterEntry;
import io.atomix.copycat.server.storage.entry.UnregisterEntry;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

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
    indexed = writer.append(1, new RegisterEntry(System.currentTimeMillis(), "test", 1000));
    assertEquals(indexed.index(), 1);
    assertEquals(indexed.term(), 1);

    indexed = writer.append(new Indexed<>(2, 1, new UnregisterEntry(System.currentTimeMillis(), 1, false), 0));
    assertEquals(indexed.index(), 2);
    assertEquals(indexed.term(), 1);

    // Test reading the register entry.
    Indexed<RegisterEntry> register;
    assertTrue(reader.hasNext());
    register = (Indexed<RegisterEntry>) reader.next();
    assertEquals(register.index(), 1);
    assertEquals(register.term(), 1);
    assertEquals(register.entry().client(), "test");
    assertEquals(register.entry().timeout(), 1000);
    assertEquals(reader.currentEntry(), register);
    assertEquals(reader.currentIndex(), 1);

    // Test reading the unregister entry.
    Indexed<UnregisterEntry> unregister;
    assertTrue(reader.hasNext());
    assertEquals(reader.nextIndex(), 2);
    unregister = (Indexed<UnregisterEntry>) reader.next();
    assertEquals(unregister.index(), 2);
    assertEquals(unregister.term(), 1);
    assertEquals(unregister.entry().session(), 1);
    assertEquals(reader.currentEntry(), unregister);
    assertEquals(reader.currentIndex(), 2);
    assertFalse(reader.hasNext());

    // Test opening a new reader and reading from the log.
    reader = log.createReader(1, Reader.Mode.ALL);
    assertTrue(reader.hasNext());
    register = (Indexed<RegisterEntry>) reader.next();
    assertEquals(register.index(), 1);
    assertEquals(register.term(), 1);
    assertEquals(register.entry().client(), "test");
    assertEquals(register.entry().timeout(), 1000);
    assertEquals(reader.currentEntry(), register);
    assertEquals(reader.currentIndex(), 1);
    assertTrue(reader.hasNext());

    assertTrue(reader.hasNext());
    assertEquals(reader.nextIndex(), 2);
    unregister = (Indexed<UnregisterEntry>) reader.next();
    assertEquals(unregister.index(), 2);
    assertEquals(unregister.term(), 1);
    assertEquals(unregister.entry().session(), 1);
    assertEquals(reader.currentEntry(), unregister);
    assertEquals(reader.currentIndex(), 2);
    assertFalse(reader.hasNext());

    // Reset the reader.
    reader.reset();

    // Test opening a new reader and reading from the log.
    reader = log.createReader(1, Reader.Mode.ALL);
    assertTrue(reader.hasNext());
    register = (Indexed<RegisterEntry>) reader.next();
    assertEquals(register.index(), 1);
    assertEquals(register.term(), 1);
    assertEquals(register.entry().client(), "test");
    assertEquals(register.entry().timeout(), 1000);
    assertEquals(reader.currentEntry(), register);
    assertEquals(reader.currentIndex(), 1);
    assertTrue(reader.hasNext());

    assertTrue(reader.hasNext());
    assertEquals(reader.nextIndex(), 2);
    unregister = (Indexed<UnregisterEntry>) reader.next();
    assertEquals(unregister.index(), 2);
    assertEquals(unregister.term(), 1);
    assertEquals(unregister.entry().session(), 1);
    assertEquals(reader.currentEntry(), unregister);
    assertEquals(reader.currentIndex(), 2);
    assertFalse(reader.hasNext());
  }
}
