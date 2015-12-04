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

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.FileBuffer;
import io.atomix.catalyst.util.Assert;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * File-based snapshot.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class FileSnapshot extends Snapshot {
  private final SnapshotFile file;
  private final SnapshotStore store;

  FileSnapshot(SnapshotFile file, SnapshotStore store) {
    super(store);
    this.file = Assert.notNull(file, "file");
    this.store = Assert.notNull(store, "store");
  }

  @Override
  public long version() {
    return file.version();
  }

  @Override
  public long timestamp() {
    return file.timestamp();
  }

  @Override
  public synchronized SnapshotWriter writer() {
    checkWriter();
    Assert.stateNot(file.file().exists(), "cannot create snapshot writer for existing snapshot file: %s", file.file());

    SnapshotDescriptor descriptor = SnapshotDescriptor.builder()
      .withVersion(file.version())
      .withTimestamp(System.currentTimeMillis())
      .build();

    Buffer buffer = FileBuffer.allocate(file.file(), SnapshotDescriptor.BYTES, store.storage.maxSnapshotSize());
    descriptor.copyTo(buffer);
    return openWriter(new SnapshotWriter(buffer.position(SnapshotDescriptor.BYTES).skip(Integer.BYTES).mark(), this), descriptor);
  }

  @Override
  protected void closeWriter(SnapshotWriter writer) {
    writer.buffer.writeLong(SnapshotDescriptor.BYTES, writer.buffer.position()).flush();
    super.closeWriter(writer);
  }

  @Override
  public synchronized SnapshotReader reader() {
    Assert.state(file.file().exists(), "cannot create snapshot reader for missing snapshot file: %s", file.file());
    Buffer buffer = FileBuffer.allocate(file.file(), SnapshotDescriptor.BYTES, store.storage.maxSnapshotSize());
    SnapshotDescriptor descriptor = new SnapshotDescriptor(buffer);
    int length = buffer.position(SnapshotDescriptor.BYTES).readInt();
    return openReader(new SnapshotReader(buffer.mark().limit(length), this), descriptor);
  }

  @Override
  public Snapshot complete() {
    Buffer buffer = FileBuffer.allocate(file.file(), SnapshotDescriptor.BYTES, store.storage.maxSnapshotSize());
    SnapshotDescriptor descriptor = new SnapshotDescriptor(buffer);
    Assert.stateNot(descriptor.locked(), "cannot complete locked snapshot descriptor");
    descriptor.lock();
    descriptor.close();
    return super.complete();
  }

  /**
   * Deletes the snapshot file.
   */
  public void delete() {
    Path path = file.file().toPath();
    if (Files.exists(path)) {
      try {
        Files.delete(file.file().toPath());
      } catch (IOException e) {
      }
    }
  }

  @Override
  public String toString() {
    return String.format("%s[version=%d]", getClass().getSimpleName(), version());
  }

}
