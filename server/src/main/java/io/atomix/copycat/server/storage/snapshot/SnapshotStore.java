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

import io.atomix.catalyst.buffer.FileBuffer;
import io.atomix.catalyst.buffer.HeapBuffer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * Persists server snapshots via the {@link Storage} module.
 * <p>
 * The server snapshot store is responsible for periodically persisting state machine state according
 * to the configured {@link Storage#level() storage level}. Each server with a snapshottable state machine
 * persists the state machine state to allow commands to be removed from disk.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SnapshotStore implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotStore.class);
  private final String name;
  final Storage storage;
  private final TreeMap<Long, Snapshot> snapshots = new TreeMap<>();
  private Snapshot currentSnapshot;

  public SnapshotStore(String name, Storage storage) {
    this.name = Assert.notNull(name, "name");
    this.storage = Assert.notNull(storage, "storage");
    open();
  }

  /**
   * Opens the snapshot manager.
   */
  private void open() {
    for (Snapshot snapshot : loadSnapshots()) {
      snapshots.put(snapshot.version(), snapshot);
    }

    if (!snapshots.isEmpty()) {
      currentSnapshot = snapshots.lastEntry().getValue();
    }
  }

  /**
   * Returns the most recent completed snapshot.
   *
   * @return The most recent completed snapshot.
   */
  public Snapshot currentSnapshot() {
    return currentSnapshot;
  }

  /**
   * Returns a collection of all snapshots.
   *
   * @return A collection of all snapshots.
   */
  public Collection<Snapshot> snapshots() {
    return snapshots.values();
  }

  /**
   * Returns a snapshot by version.
   *
   * @param version The snapshot version.
   * @return The snapshot.
   */
  public Snapshot snapshot(long version) {
    return snapshots.get(version);
  }

  /**
   * Loads all available snapshots from disk.
   *
   * @return A list of available snapshots.
   */
  private Collection<Snapshot> loadSnapshots() {
    // Ensure log directories are created.
    storage.directory().mkdirs();

    List<Snapshot> snapshots = new ArrayList<>();

    // Iterate through all files in the log directory.
    for (File file : storage.directory().listFiles(File::isFile)) {

      // If the file looks like a segment file, attempt to load the segment.
      if (SnapshotFile.isSnapshotFile(name, file)) {
        SnapshotFile snapshotFile = new SnapshotFile(file);
        SnapshotDescriptor descriptor = new SnapshotDescriptor(FileBuffer.allocate(file, SnapshotDescriptor.BYTES));

        // Valid segments will have been locked. Segments that resulting from failures during log cleaning will be
        // unlocked and should ultimately be deleted from disk.
        if (descriptor.locked()) {
          snapshots.add(loadSnapshot(descriptor.version()));
          descriptor.close();
        }
        // If the segment descriptor wasn't locked, close and delete the descriptor.
        else {
          LOGGER.debug("Deleting partial snapshot: {} ({})", descriptor.version(), snapshotFile.file().getName());
          descriptor.close();
          descriptor.delete();
        }
      }
    }

    return snapshots;
  }

  /**
   * Loads a specific snapshot.
   *
   * @param version The snapshot version.
   * @return The snapshot.
   */
  private Snapshot loadSnapshot(long version) {
    SnapshotFile file = new SnapshotFile(SnapshotFile.createSnapshotFile(name, storage.directory(), version));
    LOGGER.debug("Loaded disk snapshot: {} ({})", version, file.file().getName());
    return new FileSnapshot(file, this);
  }

  /**
   * Creates a new snapshot.
   *
   * @param version The snapshot version.
   * @return The snapshot.
   */
  public Snapshot createSnapshot(long version) {
    SnapshotDescriptor descriptor = SnapshotDescriptor.builder()
      .withVersion(version)
      .withTimestamp(System.currentTimeMillis())
      .build();
    return createSnapshot(descriptor);
  }

  /**
   * Creates a new snapshot buffer.
   */
  private Snapshot createSnapshot(SnapshotDescriptor descriptor) {
    if (storage.level() == StorageLevel.MEMORY) {
      return createMemorySnapshot(descriptor);
    } else {
      return createDiskSnapshot(descriptor);
    }
  }

  /**
   * Creates a memory snapshot.
   */
  private Snapshot createMemorySnapshot(SnapshotDescriptor descriptor) {
    HeapBuffer buffer = HeapBuffer.allocate(SnapshotDescriptor.BYTES, Integer.MAX_VALUE);
    Snapshot snapshot = new MemorySnapshot(buffer, descriptor.copyTo(buffer), this);
    LOGGER.debug("Created memory snapshot: {}", snapshot);
    return snapshot;
  }

  /**
   * Creates a disk snapshot.
   */
  private Snapshot createDiskSnapshot(SnapshotDescriptor descriptor) {
    SnapshotFile file = new SnapshotFile(SnapshotFile.createSnapshotFile(name, storage.directory(), descriptor.version()));
    Snapshot snapshot = new FileSnapshot(file, this);
    LOGGER.debug("Created disk snapshot: {}", snapshot);
    return snapshot;
  }

  /**
   * Completes writing a snapshot.
   */
  protected void completeSnapshot(Snapshot snapshot) {
    Assert.notNull(snapshot, "snapshot");
    snapshots.put(snapshot.version(), snapshot);

    if (currentSnapshot == null || snapshot.version() > currentSnapshot.version()) {
      currentSnapshot = snapshot;
    }

    // Delete old snapshots if necessary.
    if (!storage.retainStaleSnapshots()) {
      Iterator<Map.Entry<Long, Snapshot>> iterator = snapshots.entrySet().iterator();
      while (iterator.hasNext()) {
        Snapshot oldSnapshot = iterator.next().getValue();
        if (oldSnapshot.version() < snapshot.version()) {
          iterator.remove();
          oldSnapshot.close();
          oldSnapshot.delete();
        }
      }
    }
  }

  @Override
  public void close() {
  }

  /**
   * Deletes the metastore.
   */
  public void delete() {
    loadSnapshots().forEach(s -> {
      s.close();
      s.delete();
    });
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
