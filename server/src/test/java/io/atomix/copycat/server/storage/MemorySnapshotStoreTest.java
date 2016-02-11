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

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.server.storage.snapshot.SnapshotStore;
import org.testng.annotations.Test;

/**
 * Memory snapshot store test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class MemorySnapshotStoreTest extends AbstractSnapshotStoreTest {

  /**
   * Returns a new snapshot store.
   */
  protected SnapshotStore createSnapshotStore() {
    Storage storage = Storage.builder()
      .withStorageLevel(StorageLevel.MEMORY)
      .build();
    return new SnapshotStore("test", storage, new Serializer());
  }

}
