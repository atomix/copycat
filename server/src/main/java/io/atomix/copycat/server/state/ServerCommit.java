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

import io.atomix.copycat.Operation;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.storage.Indexed;
import io.atomix.copycat.server.storage.entry.OperationEntry;
import io.atomix.copycat.util.Assert;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Server commit.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class ServerCommit<T extends Operation<T>> implements Commit<T> {
  private final Indexed<? extends OperationEntry<?>> entry;
  private final ServerSessionContext session;
  private final long timestamp;
  private final AtomicInteger references = new AtomicInteger();

  public ServerCommit(Indexed<? extends OperationEntry<?>> entry, ServerSessionContext session, long timestamp) {
    this.entry = entry;
    this.session = session;
    this.timestamp = timestamp;
  }

  /**
   * Checks whether the commit is open and throws an exception if not.
   */
  private void checkOpen() {
    Assert.state(references.get() > 0, "commit not open");
  }

  @Override
  public long index() {
    checkOpen();
    return entry.index();
  }

  @Override
  public ServerSession session() {
    checkOpen();
    return session;
  }

  @Override
  public Instant time() {
    checkOpen();
    return Instant.ofEpochMilli(timestamp);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class type() {
    checkOpen();
    return entry.entry().operation().getClass();
  }

  @Override
  @SuppressWarnings("unchecked")
  public T operation() {
    checkOpen();
    return (T) entry.entry().operation();
  }

  @Override
  public Commit<T> acquire() {
    references.incrementAndGet();
    return this;
  }

  @Override
  public boolean release() {
    if (references.decrementAndGet() == 0) {
      cleanup();
      return true;
    }
    return false;
  }

  @Override
  public int references() {
    return references.get();
  }

  @Override
  public void close() {
    if (references.get() > 0) {
      references.set(0);
      cleanup();
    }
  }

  /**
   * Cleans up the commit.
   */
  private void cleanup() {
    // Clean the entry from the log.
    entry.clean();

    // Release a reference to the session.
    session.release();
  }

  @Override
  public String toString() {
    if (references() > 0) {
      return String.format("%s[index=%d, session=%s, time=%s, operation=%s]", getClass().getSimpleName(), index(), session(), time(), operation());
    } else {
      return String.format("%s[index=unknown]", getClass().getSimpleName());
    }
  }

}
