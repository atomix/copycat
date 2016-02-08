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

import io.atomix.catalyst.util.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.function.Predicate;

/**
 * Handles deletion of files.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class StorageCleaner {
  private final Storage storage;

  public StorageCleaner(Storage storage) {
    this.storage = Assert.notNull(storage, "storage");
  }

  /**
   * Cleans up files.
   */
  public void cleanFiles(Predicate<File> predicate) {
    // Ensure storage directories are created.
    storage.directory().mkdirs();

    // Iterate through all files in the storage directory.
    for (File file : storage.directory().listFiles(f -> f.isFile() && predicate.test(f))) {
      try {
        Files.delete(file.toPath());
      } catch (IOException e) {
        // Ignore the exception.
      }
    }
  }

}
