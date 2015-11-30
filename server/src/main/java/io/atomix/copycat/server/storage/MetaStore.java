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

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.FileBuffer;
import io.atomix.catalyst.buffer.HeapBuffer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.state.Member;

import java.io.File;
import java.util.Collection;

/**
 * Persists server state via the {@link Storage} module.
 * <p>
 * The server metastore is responsible for persisting server state according to the configured
 * {@link Storage#level() storage level}. Each server persists their current {@link #loadTerm() term}
 * and last {@link #loadVote() vote} as is dictated by the Raft consensus algorithm.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MetaStore implements AutoCloseable {
  private static final int TERM_POSITION = 0;
  private static final int VOTE_POSITION = 8;
  private static final int CONFIGURATION_POSITION = 16;
  private final Storage storage;
  private final Buffer buffer;

  MetaStore(String name, Storage storage) {
    this.storage = Assert.notNull(storage, "storage");
    if (storage.level() == StorageLevel.MEMORY) {
      buffer = HeapBuffer.allocate(24);
    } else {
      storage.directory().mkdirs();
      File file = new File(storage.directory(), String.format("%s.meta", name));
      buffer = FileBuffer.allocate(file, 24);
    }
  }

  /**
   * Stores the current server term.
   *
   * @param term The current server term.
   * @return The metastore.
   */
  public synchronized MetaStore storeTerm(long term) {
    buffer.writeLong(TERM_POSITION, term);
    return this;
  }

  /**
   * Loads the stored server term.
   *
   * @return The stored server term.
   */
  public synchronized long loadTerm() {
    return buffer.readLong(TERM_POSITION);
  }

  /**
   * Stores the last voted server.
   *
   * @param vote The server vote.
   * @return The metastore.
   */
  public synchronized MetaStore storeVote(int vote) {
    buffer.writeInt(VOTE_POSITION, vote);
    return this;
  }

  /**
   * Loads the last vote for the server.
   *
   * @return The last vote for the server.
   */
  public synchronized int loadVote() {
    return buffer.readInt(VOTE_POSITION);
  }

  /**
   * Stores a snapshot.
   *
   * @param snapshot The snapshot to store.
   * @return The metastore.
   */
  public synchronized MetaStore storeSnapshot(Snapshot snapshot) {
    final int configurationLength = buffer.readInt(CONFIGURATION_POSITION);
    final int snapshotLengthPosition = CONFIGURATION_POSITION + Integer.BYTES + configurationLength;
    final int snapshotStartPosition = snapshotLengthPosition + Integer.BYTES;
    buffer.position(snapshotStartPosition).writeLong(snapshot.version).write(snapshot.data);
    final int snapshotLength = (int) (buffer.position() - snapshotStartPosition);
    buffer.writeInt(snapshotLengthPosition, snapshotLength);
    return this;
  }

  /**
   * Loads the current snapshot.
   *
   * @return The current snapshot.
   */
  public synchronized Snapshot loadSnapshot() {
    final int configurationLength = buffer.readInt(CONFIGURATION_POSITION);
    final int snapshotLengthPosition = CONFIGURATION_POSITION + Integer.BYTES + configurationLength;
    final int snapshotStartPosition = snapshotLengthPosition + Integer.BYTES;
    final int snapshotLength = buffer.readInt(snapshotLengthPosition);
    if (snapshotLength > 0) {
      long version = buffer.readLong(snapshotStartPosition);
      Buffer data = HeapBuffer.allocate();
      buffer.position(snapshotStartPosition + Long.BYTES)
        .read(data.limit(snapshotLength - Long.BYTES));
      return new Snapshot(version, data.flip());
    }
    return null;
  }

  /**
   * Stores the current cluster configuration.
   *
   * @param configuration The current cluster configuration.
   * @return The metastore.
   */
  public synchronized MetaStore storeConfiguration(Configuration configuration) {
    Snapshot snapshot = loadSnapshot();
    final int configurationLengthPosition = CONFIGURATION_POSITION;
    final int configurationStartPosition = configurationLengthPosition + Integer.BYTES;
    buffer.position(configurationStartPosition).writeLong(configuration.version);
    storage.serializer().writeObject(configuration.members, buffer);
    final int configurationLength = (int) (buffer.position() - configurationStartPosition);
    buffer.writeInt(configurationLengthPosition, configurationLength);
    if (snapshot != null) {
      storeSnapshot(snapshot);
    }
    return this;
  }

  /**
   * Loads the current cluster configuration.
   *
   * @return The current cluster configuration.
   */
  public synchronized Configuration loadConfiguration() {
    final int configurationLength = buffer.position(CONFIGURATION_POSITION).readInt();
    if (configurationLength > 0) {
      long version = buffer.readLong();
      try (Buffer slice = buffer.slice(configurationLength)) {
        return new Configuration(version, storage.serializer().readObject(slice));
      }
    }
    return null;
  }

  @Override
  public void close() {
    buffer.close();
  }

  /**
   * Deletes the metastore.
   */
  public void delete() {
    if (buffer instanceof FileBuffer) {
      ((FileBuffer) buffer).delete();
    }
  }

  @Override
  public String toString() {
    if (buffer instanceof FileBuffer) {
      return String.format("%s[%s]", getClass().getSimpleName(), ((FileBuffer) buffer).file());
    } else {
      return getClass().getSimpleName();
    }
  }

  /**
   * Metastore configuration.
   */
  public static class Configuration {
    private final long version;
    private final Collection<Member> members;

    public Configuration(long version, Collection<Member> members) {
      this.version = version;
      this.members = Assert.notNull(members, "members");
    }

    /**
     * Returns the configuration version.
     *
     * @return The configuration version.
     */
    public long version() {
      return version;
    }

    /**
     * Returns the collection of active members.
     *
     * @return The collection of active members.
     */
    public Collection<Member> members() {
      return members;
    }

    @Override
    public String toString() {
      return String.format("%s[members=%s]", getClass().getSimpleName(), members);
    }
  }

  /**
   * Metastore snapshot.
   */
  public static class Snapshot implements AutoCloseable {
    private final long version;
    private final Buffer data;

    public Snapshot(long version, Buffer data) {
      this.version = version;
      this.data = data;
    }

    /**
     * Returns the snapshot version.
     *
     * @return The snapshot version.
     */
    public long version() {
      return version;
    }

    /**
     * Returns the snapshot data.
     *
     * @return The snapshot data.
     */
    public Buffer data() {
      return data;
    }

    @Override
    public void close() {
      data.close();
    }
  }

}
