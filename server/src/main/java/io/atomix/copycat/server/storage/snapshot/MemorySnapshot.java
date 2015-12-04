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

import io.atomix.catalyst.buffer.HeapBuffer;
import io.atomix.catalyst.util.Assert;

/**
 * In-memory snapshot.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class MemorySnapshot extends Snapshot {
  private final HeapBuffer buffer;
  private final SnapshotDescriptor descriptor;

  MemorySnapshot(HeapBuffer buffer, SnapshotDescriptor descriptor, SnapshotStore store) {
    super(store);
    buffer.mark();
    this.buffer = Assert.notNull(buffer, "buffer");
    this.descriptor = Assert.notNull(descriptor, "descriptor");
  }

  @Override
  public long version() {
    return descriptor.version();
  }

  @Override
  public long timestamp() {
    return descriptor.timestamp();
  }

  @Override
  public SnapshotWriter writer() {
    checkWriter();
    return new SnapshotWriter(buffer.position(SnapshotDescriptor.BYTES).slice(), this);
  }

  @Override
  protected void closeWriter(SnapshotWriter writer) {
    writer.buffer.capacity(writer.buffer.position()).limit(writer.buffer.position());
    super.closeWriter(writer);
  }

  @Override
  public synchronized SnapshotReader reader() {
    return openReader(new SnapshotReader(buffer.position(SnapshotDescriptor.BYTES).slice(), this), descriptor);
  }

  @Override
  public Snapshot complete() {
    descriptor.lock();
    return super.complete();
  }

  @Override
  public void close() {
    buffer.close();
  }

  @Override
  public String toString() {
    return String.format("%s[version=%d]", getClass().getSimpleName(), descriptor.version());
  }

}
