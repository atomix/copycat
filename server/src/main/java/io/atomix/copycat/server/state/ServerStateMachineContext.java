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
import io.atomix.copycat.server.CopycatServer;
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
  private CopycatServer.State state = CopycatServer.State.INACTIVE;
  private final ServerClock clock = new ServerClock();
  private final ConnectionManager connections;
  private final ServerSessionManager sessions;
  private long version;
  private boolean synchronous;
  private Command.ConsistencyLevel consistency;
  private final List<CompletableFuture<Void>> futures = new ArrayList<>();

  public ServerStateMachineContext(ConnectionManager connections, ServerSessionManager sessions) {
    this.connections = connections;
    this.sessions = sessions;
  }

  /**
   * Returns the server state.
   *
   * @return The server state.
   */
  CopycatServer.State state() {
    return state;
  }

  /**
   * Updates the state machine context.
   */
  void update(long index, Instant instant, boolean synchronous, Command.ConsistencyLevel consistency) {
    version = index;
    clock.set(instant);
    this.synchronous = synchronous;
    this.consistency = consistency;
    futures.clear();
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

  /**
   * Registers a session event future in the context.
   */
  void register(CompletableFuture<Void> future) {
    futures.add(future);
  }

  /**
   * Returns futures registered for the context.
   */
  CompletableFuture<Void> futures() {
    return !futures.isEmpty() ? CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])) : null;
  }


  @Override
  public long version() {
    return version;
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
    return String.format("%s[version=%d, time=%s]", getClass().getSimpleName(), version, clock);
  }

}
