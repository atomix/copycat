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

import io.atomix.copycat.server.storage.entry.OperationEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Server commit pool.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ServerCommitPool implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerCommitPool.class);
  private final ServerCommitCleaner cleaner;
  private final ServerSessionManager sessions;
  private final Queue<ServerCommit> pool = new ConcurrentLinkedQueue<>();

  public ServerCommitPool(ServerCommitCleaner cleaner, ServerSessionManager sessions) {
    this.cleaner = cleaner;
    this.sessions = sessions;
  }

  /**
   * Acquires a commit from the pool.
   *
   * @param entry The entry for which to acquire the commit.
   * @return The commit to acquire.
   */
  public ServerCommit acquire(OperationEntry entry, long timestamp) {
    ServerCommit commit = pool.poll();
    if (commit == null) {
      commit = new ServerCommit(this, cleaner, sessions);
    }
    commit.reset(entry, timestamp);
    return commit;
  }

  /**
   * Releases a commit back to the pool.
   *
   * @param commit The commit to release.
   */
  public void release(ServerCommit commit) {
    pool.add(commit);
  }

  /**
   * Issues a warning that the given commit was garbage collected.
   *
   * @param commit The commit that was garbage collected.
   */
  public void warn(ServerCommit commit) {
    LOGGER.warn("Server commit " + commit + " was garbage collected!\nCommit log is dirty!");
  }

  @Override
  public void close() {
    pool.clear();
  }

}
