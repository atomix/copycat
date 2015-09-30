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

import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Operation;
import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.storage.entry.OperationEntry;
import io.atomix.catalyst.util.Assert;

import java.time.Instant;

/**
 * Server commit.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ServerCommit implements Commit<Operation<?>> {
  private final ServerCommitPool pool;
  private final ServerCommitCleaner cleaner;
  private final ServerSessionManager sessions;
  private OperationEntry<?> entry;
  private Session session;
  private Instant instant;
  private volatile boolean open;

  public ServerCommit(ServerCommitPool pool, ServerCommitCleaner cleaner, ServerSessionManager sessions) {
    this.pool = pool;
    this.cleaner = cleaner;
    this.sessions = sessions;
  }

  /**
   * Resets the commit.
   *
   * @param entry The entry.
   */
  void reset(OperationEntry<?> entry) {
    entry.acquire();
    this.entry = entry;
    this.session = sessions.getSession(entry.getSession());
    this.instant = Instant.ofEpochMilli(entry.getTimestamp());
    open = true;
  }

  @Override
  public long index() {
    return entry != null ? entry.getIndex() : 0;
  }

  @Override
  public Session session() {
    return session;
  }

  @Override
  public Instant time() {
    return instant;
  }

  @Override
  public Class type() {
    return entry != null ? entry.getOperation().getClass() : null;
  }

  @Override
  public Operation<?> operation() {
    return entry != null ? entry.getOperation() : null;
  }

  @Override
  public void clean() {
    Assert.state(open, "commit closed");
    if (entry.getOperation() instanceof Command)
      cleaner.clean(entry);
    close();
  }

  @Override
  public void clean(boolean tombstone) {
    Assert.state(open, "commit closed");
    if (entry.getOperation() instanceof Command)
      cleaner.clean(entry, tombstone);
    close();
  }

  @Override
  public void close() {
    if (open) {
      entry.release();
      entry = null;
      pool.release(this);
      open = false;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    pool.warn(this);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, session=%s, time=%s, operation=%s]", getClass().getSimpleName(), index(), session(), time(), operation());
  }

}
