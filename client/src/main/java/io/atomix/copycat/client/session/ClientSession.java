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
package io.atomix.copycat.client.session;

import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Managed;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.ConnectionStrategy;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.RetryStrategy;
import io.atomix.copycat.client.util.AddressSelector;
import io.atomix.copycat.client.util.ClientConnection;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Client session.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ClientSession implements Session, Managed<Session> {
  private final ClientSessionState state;
  private final ThreadContext context;
  private final ClientSessionManager manager;
  private final ClientSessionListener listener;
  private final ClientSessionSubmitter submitter;
  private final Set<ManagerListener> openListeners = new CopyOnWriteArraySet<>();
  private final Set<ManagerListener> closeListeners = new CopyOnWriteArraySet<>();

  public ClientSession(Client client, AddressSelector selector, ThreadContext context, ConnectionStrategy connectionStrategy, RetryStrategy retryStrategy) {
    this(UUID.randomUUID(), client, selector, context, connectionStrategy, retryStrategy);
  }

  public ClientSession(UUID id, Client client, AddressSelector selector, ThreadContext context, ConnectionStrategy connectionStrategy, RetryStrategy retryStrategy) {
    this(new ClientConnection(id, client, selector), selector, new ClientSessionState(id), context, connectionStrategy, retryStrategy);
  }

  private ClientSession(ClientConnection connection, AddressSelector selector, ClientSessionState state, ThreadContext context, ConnectionStrategy connectionStrategy, RetryStrategy retryStrategy) {
    Assert.notNull(connection, "connection");
    this.state = Assert.notNull(state, "state");
    this.context = Assert.notNull(context, "context");
    this.manager = new ClientSessionManager(connection, selector, state, context, connectionStrategy);
    this.listener = new ClientSessionListener(connection, state, context);
    this.submitter = new ClientSessionSubmitter(connection, state, context, retryStrategy);
  }

  @Override
  public long id() {
    return state.getSessionId();
  }

  @Override
  public State state() {
    return state.getState();
  }

  @Override
  public Listener<State> onStateChange(Consumer<State> callback) {
    return state.onStateChange(callback);
  }

  /**
   * Submits a command to the session.
   *
   * @param command The command to submit.
   * @param <T> The command result type.
   * @return A completable future to be completed with the command result.
   */
  public <T> CompletableFuture<T> submit(Command<T> command) {
    return submitter.submit(command);
  }

  /**
   * Submits a query to the session.
   *
   * @param query The query to submit.
   * @param <T> The query result type.
   * @return A completable future to be completed with the query result.
   */
  public <T> CompletableFuture<T> submit(Query<T> query) {
    return submitter.submit(query);
  }

  /**
   * Opens the session.
   *
   * @return A completable future to be completed once the session is opened.
   */
  public CompletableFuture<Session> open() {
    return manager.open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return state.getState() == State.OPEN || state.getState() == State.UNSTABLE;
  }

  @Override
  public Session publish(String event) {
    listener.publish(event);
    return this;
  }

  @Override
  public Session publish(String event, Object message) {
    listener.publish(event, message);
    return this;
  }

  @Override
  public Listener<Void> onEvent(String event, Runnable callback) {
    return listener.onEvent(event, callback);
  }

  @Override
  public <T> Listener<T> onEvent(String event, Consumer<T> callback) {
    return listener.onEvent(event, callback);
  }

  /**
   * Closes the session.
   *
   * @return A completable future to be completed once the session is closed.
   */
  public CompletableFuture<Void> close() {
    ThreadContext context = ThreadContext.currentContext();
    if (context != null) {
      return submitter.close()
        .thenCompose(v -> listener.close())
        .thenCompose(v -> manager.close())
        .whenCompleteAsync((result, error) -> this.context.close(), context.executor());
    } else {
      return submitter.close()
        .thenCompose(v -> listener.close())
        .thenCompose(v -> manager.close())
        .whenCompleteAsync((result, error) -> this.context.close());
    }
  }

  @Override
  public boolean isClosed() {
    return state.getState() == State.EXPIRED || state.getState() == State.CLOSED;
  }

  /**
   * Manager listener.
   */
  private abstract class ManagerListener implements Listener<Session> {
    private final Consumer<Session> callback;

    private ManagerListener(Consumer<Session> callback) {
      this.callback = callback;
    }

    @Override
    public void accept(Session session) {
      callback.accept(session);
    }
  }

  /**
   * Session open listener.
   */
  private final class OpenListener extends ManagerListener {
    public OpenListener(Consumer<Session> callback) {
      super(callback);
      openListeners.add(this);
    }

    @Override
    public void close() {
      openListeners.remove(this);
    }
  }

  /**
   * Session close listener.
   */
  private final class CloseListener extends ManagerListener {
    public CloseListener(Consumer<Session> callback) {
      super(callback);
      closeListeners.add(this);
    }

    @Override
    public void close() {
      closeListeners.remove(this);
    }
  }

}
