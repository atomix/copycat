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

package io.atomix.catalogue.server.state;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import io.atomix.catalogue.server.StateMachineContext;

/**
 * Server state machine context.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ServerStateMachineContext implements StateMachineContext {
  private long version;
  private Type type = Type.NONE;
  private final ServerClock clock = new ServerClock();
  private final ServerSessionManager sessions = new ServerSessionManager(this);
  private final List<CompletableFuture<Void>> futures = new ArrayList<>();

  /**
   * State machine context type.
   */
  enum Type {
    COMMAND,
    QUERY,
    NONE
  }

  /**
   * Updates the state machine context.
   */
  void update(long index, Instant instant, Type type) {
    version = index;
    clock.set(instant);
    this.type = type;
    futures.clear();
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

  /**
   * Returns the current context type.
   *
   * @return The current context type.
   */
  Type type() {
    return type;
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
  public Instant now() {
    return clock.instant();
  }

  @Override
  public ServerSessionManager sessions() {
    return sessions;
  }

  @Override
  public String toString() {
    return String.format("%s[version=%d, time=%s]", getClass().getSimpleName(), version, clock);
  }

}
