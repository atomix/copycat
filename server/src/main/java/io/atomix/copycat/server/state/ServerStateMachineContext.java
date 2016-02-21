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

import io.atomix.copycat.Command;
import io.atomix.copycat.server.StateMachineContext;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Server state machine context.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ServerStateMachineContext implements StateMachineContext {
  private final ServerClock clock = new ServerClock();
  private final ConnectionManager connections;
  private final ServerSessionManager sessions;
  private long index;
  private boolean synchronous;
  private Command.ConsistencyLevel consistency;

  public ServerStateMachineContext(ConnectionManager connections, ServerSessionManager sessions) {
    this.connections = connections;
    this.sessions = sessions;
  }

  /**
   * Updates the state machine context.
   */
  void update(long index, Instant instant, boolean synchronous, Command.ConsistencyLevel consistency) {
    this.index = index;
    clock.set(instant);
    this.synchronous = synchronous;
    this.consistency = consistency;
  }

  /**
   * Commits the state machine index.
   */
  CompletableFuture<Void> commit() {
    long index = this.index;

    List<CompletableFuture<Void>> futures = null;
    for (ServerSessionContext session : sessions.sessions.values()) {
      CompletableFuture<Void> future = session.commit(index);
      if (future != null) {
        if (futures == null)
          futures = new ArrayList<>(8);
        futures.add(future);
      }
    }

    return futures != null ? CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])) : null;
  }

  /**
   * Indicates whether the current context is synchronous.
   */
  boolean synchronous() {
    return synchronous;
  }

  /**
   * Returns the context consistency level.
   */
  Command.ConsistencyLevel consistency() {
    return consistency;
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public Clock clock() {
    return clock;
  }

  @Override
  public ServerSessionManager sessions() {
    return sessions;
  }

  /**
   * Returns the server connections.
   */
  ConnectionManager connections() {
    return connections;
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, time=%s]", getClass().getSimpleName(), index, clock);
  }

}
