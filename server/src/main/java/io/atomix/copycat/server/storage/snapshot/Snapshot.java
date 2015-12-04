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

/**
 * Stored server snapshot.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class Snapshot implements AutoCloseable {
  private final SnapshotStore store;
  private SnapshotWriter writer;

  protected Snapshot(SnapshotStore store) {
    this.store = Assert.notNull(store, "store");
  }

  /**
   * Returns the snapshot version.
   *
   * @return The snapshot version.
   */
  public abstract long version();

  /**
   * Returns the snapshot timestamp.
   *
   * @return The snapshot timestamp.
   */
  public abstract long timestamp();

  /**
   * Returns a new snapshot writer.
   *
   * @return A new snapshot writer.
   */
  public abstract SnapshotWriter writer();

  /**
   * Checks that the snapshot can be written.
   */
  protected void checkWriter() {
    Assert.state(writer == null, "cannot create multiple writers for the same snapshot");
  }

  /**
   * Opens the given snapshot writer.
   */
  protected SnapshotWriter openWriter(SnapshotWriter writer, SnapshotDescriptor descriptor) {
    checkWriter();
    Assert.stateNot(descriptor.locked(), "cannot write to locked snapshot descriptor");
    this.writer = Assert.notNull(writer, "writer");
    return writer;
  }

  /**
   * Closes the current snapshot writer.
   */
  protected void closeWriter(SnapshotWriter writer) {
    this.writer = null;
  }

  /**
   * Returns a new snapshot reader.
   *
   * @return A new snapshot reader.
   */
  public abstract SnapshotReader reader();

  /**
   * Opens the given snapshot reader.
   */
  protected SnapshotReader openReader(SnapshotReader reader, SnapshotDescriptor descriptor) {
    Assert.state(descriptor.locked(), "cannot read from unlocked snapshot descriptor");
    return reader;
  }

  /**
   * Closes the current snapshot reader.
   */
  protected void closeReader(SnapshotReader reader) {
    reader = null;
  }

  /**
   * Completes the snapshot.
   *
   * @return The completed snapshot.
   */
  public Snapshot complete() {
    store.completeSnapshot(this);
    return this;
  }

  /**
   * Closes the snapshot.
   */
  @Override
  public void close() {
  }

  /**
   * Deletes the snapshot.
   */
  public void delete() {
  }

}
