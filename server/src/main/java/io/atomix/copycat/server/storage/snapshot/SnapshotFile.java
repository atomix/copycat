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
package io.atomix.copycat.server.storage.snapshot;

import io.atomix.catalyst.util.Assert;

import java.io.File;

/**
 * Snapshot file utility.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class SnapshotFile {
  private final File file;

  /**
   * Returns a boolean value indicating whether the given file appears to be a parsable snapshot file.
   *
   * @throws NullPointerException if {@code file} is null
   */
  static boolean isSnapshotFile(String name, File file) {
    Assert.notNull(name, "name");
    Assert.notNull(file, "file");
    String fileName = file.getName();
    if (fileName.lastIndexOf('.') == -1 || fileName.lastIndexOf('-') == -1 || fileName.lastIndexOf('.') < fileName.lastIndexOf('-') || !fileName.endsWith(".snapshot"))
      return false;

    for (int i = fileName.lastIndexOf('-') + 1; i < fileName.lastIndexOf('.'); i++) {
      if (!Character.isDigit(fileName.charAt(i))) {
        return false;
      }
    }
    return fileName.substring(0, fileName.lastIndexOf('-')).equals(name);
  }

  /**
   * Creates a snapshot file for the given directory, log name, and snapshot version.
   */
  static File createSnapshotFile(String name, File directory, long version) {
    return new File(directory, String.format("%s-%d.snapshot", Assert.notNull(name, "name"), version));
  }

  /**
   * @throws IllegalArgumentException if {@code file} is not a valid snapshot file
   */
  SnapshotFile(File file) {
    this.file = file;
  }

  /**
   * Returns the snapshot file.
   *
   * @return The snapshot file.
   */
  public File file() {
    return file;
  }

  /**
   * Returns the snapshot version.
   */
  public long version() {
    return Long.valueOf(file.getName().substring(file.getName().lastIndexOf('-') + 1, file.getName().lastIndexOf('.')));
  }

}
