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
package io.atomix.copycat.server.controller;

import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.catalyst.util.Managed;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.cluster.MemberType;
import io.atomix.copycat.server.state.ServerState;
import io.atomix.copycat.server.state.ServerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * Server state controller.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class ServerStateController<T extends ServerState> implements Managed<ServerStateController<T>> {
  private final Logger LOGGER = LoggerFactory.getLogger(getClass());
  private final Listeners<CopycatServer.State> stateChangeListeners = new Listeners<>();
  protected final ServerContext context;
  protected T state;
  private boolean open;

  protected ServerStateController(ServerContext context) {
    this.context = Assert.notNull(context, "context");
  }

  /**
   * Returns the controller member type.
   *
   * @return The controller member type.
   */
  public abstract MemberType type();

  /**
   * Returns the controller state.
   *
   * @return The current controller state.
   */
  public ServerState state() {
    return state;
  }

  /**
   * Returns the server state context.
   *
   * @return The server state context.
   */
  public ServerContext context() {
    return context;
  }

  /**
   * Registers a state change listener.
   *
   * @param listener The state change listener.
   * @return The listener context.
   */
  public Listener<CopycatServer.State> onStateChange(Consumer<CopycatServer.State> listener) {
    return stateChangeListeners.add(listener);
  }

  /**
   * Connects a client connection.
   */
  public abstract void connectClient(Connection connection);

  /**
   * Disconnects a client connection.
   */
  public abstract void disconnectClient(Connection connection);

  /**
   * Connects a server connection.
   */
  public abstract void connectServer(Connection connection);

  /**
   * Disconnects a server connection.
   */
  public abstract void disconnectServer(Connection connection);

  /**
   * Transitions the server state.
   */
  public void transition(ServerState.Type<T> state) {
    context.checkThread();

    // If the state has not changed, return.
    if (this.state != null && state == this.state.type())
      return;

    LOGGER.info("{} - Transitioning to {}", context.getCluster().getMember().serverAddress(), state);

    // Close the current state.
    if (this.state != null) {
      try {
        this.state.close().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IllegalStateException("failed to close Raft state", e);
      }
    }

    // Force state transitions to occur synchronously in order to prevent race conditions.
    try {
      this.state = state.createState(this);
      this.state.open().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to initialize Raft state", e);
    }

    stateChangeListeners.forEach(l -> l.accept(this.state.type()));
  }

  @Override
  public CompletableFuture<ServerStateController<T>> open() {
    open = true;
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  @Override
  public CompletableFuture<Void> close() {
    open = false;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

}
