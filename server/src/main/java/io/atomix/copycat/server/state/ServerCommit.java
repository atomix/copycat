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

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Operation;
import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.storage.entry.OperationEntry;

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
  private long index;
  private Session session;
  private Instant instant;
  private Operation operation;
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
    this.index = entry.getIndex();
    this.session = sessions.getSession(entry.getSession());
    this.instant = Instant.ofEpochMilli(entry.getTimestamp());
    this.operation = entry.getOperation();
    open = true;
  }

  @Override
  public long index() {
    return index;
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
    return operation != null ? operation.getClass() : null;
  }

  @Override
  public Operation<?> operation() {
    return operation;
  }

  @Override
  public void clean() {
    Assert.state(open, "commit closed");
    if (operation instanceof Command)
      cleaner.clean(index);
    close();
  }

  @Override
  public void close() {
    if (open) {
      index = 0;
      session = null;
      instant = null;
      operation = null;
      pool.release(this);
      open = false;
    }
  }

  @Override
  protected void finalize() throws Throwable {
    pool.warn(this);
    super.finalize();
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, session=%s, time=%s, operation=%s]", getClass().getSimpleName(), index(), session(), time(), operation());
  }

}
