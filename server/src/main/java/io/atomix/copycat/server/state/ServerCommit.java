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
import io.atomix.copycat.Command;
import io.atomix.copycat.Operation;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.entry.OperationEntry;

import java.time.Instant;

/**
 * Server commit.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class ServerCommit implements Commit<Operation<?>> {
  private final ServerCommitPool pool;
  private final Log log;
  private long index;
  private ServerSessionContext session;
  private Instant instant;
  private Operation operation;
  private volatile boolean open;

  public ServerCommit(ServerCommitPool pool, Log log) {
    this.pool = pool;
    this.log = log;
  }

  /**
   * Resets the commit.
   *
   * @param entry The entry.
   */
  void reset(OperationEntry<?> entry, ServerSessionContext session, long timestamp) {
    this.index = entry.getIndex();
    this.session = session;
    this.instant = Instant.ofEpochMilli(timestamp);
    this.operation = entry.getOperation();
    session.acquire();
    open = true;
  }

  /**
   * Checks whether the commit is open and throws an exception if not.
   */
  private void checkOpen() {
    Assert.state(open, "commit not open");
  }

  @Override
  public long index() {
    checkOpen();
    return index;
  }

  @Override
  public ServerSession session() {
    checkOpen();
    return session;
  }

  @Override
  public Instant time() {
    checkOpen();
    return instant;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Class type() {
    checkOpen();
    return operation != null ? operation.getClass() : null;
  }

  @Override
  public Operation<?> operation() {
    checkOpen();
    return operation;
  }

  @Override
  public void close() {
    if (open) {
      if (operation instanceof Command && log.isOpen()) {
        try {
          log.clean(index);
        } catch (IllegalStateException e) {
        }
      }

      session.release();

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
    if (open) {
      return String.format("%s[index=%d, session=%s, time=%s, operation=%s]", getClass().getSimpleName(), index(), session(), time(), operation());
    } else {
      return String.format("%s[index=unknown]", getClass().getSimpleName());
    }
  }

}
