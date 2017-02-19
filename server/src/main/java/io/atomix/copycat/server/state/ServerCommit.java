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
 * limitations under the License.
 */
package io.atomix.copycat.server.state;

import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.Session;
import io.atomix.copycat.server.storage.Indexed;
import io.atomix.copycat.server.storage.compaction.Compaction;
import io.atomix.copycat.server.storage.entry.OperationEntry;
import io.atomix.copycat.util.Assert;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.HeapBuffer;

import java.time.Instant;

/**
 * Server commit.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class ServerCommit implements Commit {
  private final Type type;
  private final Indexed<? extends OperationEntry<?>> entry;
  private final ServerSession session;
  private final long timestamp;
  private boolean open = true;

  ServerCommit(Type type, Indexed<? extends OperationEntry<?>> entry, ServerSession session, long timestamp) {
    this.type = type;
    this.entry = entry;
    this.session = session;
    this.timestamp = timestamp;
  }

  /**
   * Checks whether the commit is open and throws an exception if not.
   */
  private void checkOpen() {
    Assert.state(open, "commit not open");
  }

  @Override
  public Type type() {
    return type;
  }

  @Override
  public long index() {
    checkOpen();
    return entry.index();
  }

  @Override
  public Session session() {
    checkOpen();
    return session;
  }

  @Override
  public Instant time() {
    checkOpen();
    return Instant.ofEpochMilli(timestamp);
  }

  @Override
  public BufferInput buffer() {
    return HeapBuffer.wrap(entry.entry().bytes());
  }

  @Override
  public void compact(CompactionMode mode) {
    // Force close the commit on compact.
    close();

    // Compact the entry from the log.
    switch (mode) {
      case RELEASE:
      case QUORUM:
        entry.compact(Compaction.Mode.QUORUM);
        break;
      case SEQUENTIAL:
      case EXPIRING:
      case TOMBSTONE:
        entry.compact(Compaction.Mode.SEQUENTIAL);
        break;
      case SNAPSHOT:
        entry.compact(Compaction.Mode.SNAPSHOT);
        break;
    }

    // Release the reference to the commit in the parent session.
    session.release();
  }

  @Override
  public void close() {
    open = false;
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, session=%s, time=%s, operation=%s]", getClass().getSimpleName(), index(), session(), time(), buffer());
  }

}
