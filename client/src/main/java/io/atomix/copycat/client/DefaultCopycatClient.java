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
package io.atomix.copycat.client;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.concurrent.CatalystThreadFactory;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.SingleThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.session.ClientSession;
import io.atomix.copycat.client.session.ClosedSessionException;
import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.client.util.AddressSelector;
import io.atomix.copycat.client.util.ClientSequencer;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Default Copycat client implementation.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class DefaultCopycatClient implements CopycatClient {
  private final Transport transport;
  private final Serializer serializer;
  private final CatalystThreadFactory threadFactory = new CatalystThreadFactory("copycat-client-%d");
  private final ThreadContext context;
  private final AddressSelector selector;
  private final ConnectionStrategy connectionStrategy;
  private final RetryStrategy retryStrategy;
  private final RecoveryStrategy recoveryStrategy;
  private final ClientSequencer sequencer = new ClientSequencer();
  private ClientSession session;
  private State state;
  private final Map<Long, OperationFuture<?>> operations = new LinkedHashMap<>();
  private final Set<StateChangeListener> changeListeners = new CopyOnWriteArraySet<>();
  private final Set<EventListener<?>> eventListeners = new CopyOnWriteArraySet<>();
  private Listener<Session.State> changeListener;

  public DefaultCopycatClient(Transport transport, Collection<Address> members, Serializer serializer, ServerSelectionStrategy selectionStrategy, ConnectionStrategy connectionStrategy, RetryStrategy retryStrategy, RecoveryStrategy recoveryStrategy) {
    this.transport = Assert.notNull(transport, "transport");
    this.serializer = Assert.notNull(serializer, "serializer");
    this.context = new SingleThreadContext(threadFactory, serializer.clone());
    this.selector = new AddressSelector(members, selectionStrategy);
    this.connectionStrategy = Assert.notNull(connectionStrategy, "connectionStrategy");
    this.retryStrategy = Assert.notNull(retryStrategy, "retryStrategy");
    this.recoveryStrategy = Assert.notNull(recoveryStrategy, "recoveryStrategy");
  }

  @Override
  public State state() {
    return state;
  }

  /**
   * Updates the client state.
   */
  private void setState(State state) {
    if (this.state != state) {
      this.state = state;
      changeListeners.forEach(l -> l.accept(state));
    }
  }

  @Override
  public Listener<State> onStateChange(Consumer<State> callback) {
    return new StateChangeListener(callback);
  }

  @Override
  public Transport transport() {
    return transport;
  }

  @Override
  public Serializer serializer() {
    ThreadContext context = ThreadContext.currentContext();
    return context != null ? context.serializer() : serializer;
  }

  @Override
  public Session session() {
    return session;
  }

  @Override
  public ThreadContext context() {
    return context;
  }

  /**
   * Creates a new child session.
   */
  private ClientSession newSession() {
    session = new ClientSession(transport.client(), selector, new SingleThreadContext(threadFactory, serializer.clone()), connectionStrategy, retryStrategy);

    // Update the session change listener.
    if (changeListener != null)
      changeListener.close();
    changeListener = session.onStateChange(this::onStateChange);

    // Register all event listeners.
    eventListeners.forEach(EventListener::register);
    return session;
  }

  /**
   * Handles a session state change.
   */
  private void onStateChange(Session.State state) {
    switch (state) {
      // When the session is opened, transition the state to CONNECTED.
      case OPEN:
        setState(State.CONNECTED);
        break;
      // When the session becomes unstable, transition the state to SUSPENDED.
      case UNSTABLE:
        setState(State.SUSPENDED);
        break;
      // When the session is expired or closed, transition the state to SUSPENDED if necessary. We don't
      // transition to CLOSED here because the recovery strategy must determine whether to close or recover
      // the client's session.
      case EXPIRED:
      case CLOSED:
        setState(State.SUSPENDED);
        recoveryStrategy.recover(this);
        break;
    }
  }

  @Override
  public CompletableFuture<CopycatClient> open() {
    if (state != State.CLOSED)
      return CompletableFuture.completedFuture(this);
    return newSession().open().thenApply(v -> this);
  }

  @Override
  public boolean isOpen() {
    return state != State.CLOSED;
  }

  @Override
  public <T> CompletableFuture<T> submit(Command<T> command) {
    if (session == null)
      return Futures.exceptionalFuture(new ClosedSessionException("session closed"));

    OperationFuture<T> future = new OperationFuture<>(command);
    context.executor().execute(() -> submit(command, session::submit, future));
    return future;
  }

  @Override
  public <T> CompletableFuture<T> submit(Query<T> query) {
    if (session == null)
      return Futures.exceptionalFuture(new ClosedSessionException("session closed"));

    OperationFuture<T> future = new OperationFuture<>(query);
    context.executor().execute(() -> submit(query, session::submit, future));
    return future;
  }

  /**
   * Submits an operation to the cluster.
   */
  private <T extends Operation<U>, U> void submit(T operation, Function<T, CompletableFuture<U>> submitter, OperationFuture<U> future) {
    long sequence = sequencer.nextSequence();
    operations.put(sequence, future);
    submitter.apply(operation).whenComplete((result, error) -> {
      sequencer.sequence(sequence, () -> {
        if (error == null) {
          operations.remove(sequence);
          future.complete(result);
        } else if (!(error instanceof ClosedSessionException)) {
          operations.remove(sequence);
          future.completeExceptionally(error);
        }
      });
    });
  }

  @Override
  public Listener<Void> onEvent(String event, Runnable callback) {
    return onEvent(event, v -> callback.run());
  }

  @Override
  public <T> Listener<T> onEvent(String event, Consumer<T> callback) {
    EventListener<T> listener = new EventListener<>(event, callback);
    listener.register();
    return listener;
  }

  @Override
  public CompletableFuture<CopycatClient> recover() {
    if (state != State.SUSPENDED)
      return Futures.exceptionalFuture(new IllegalStateException("cannot recover client in " + state + " state"));

    // Open the new child session. If an exception occurs opening the new child session, consider this session expired.
    CompletableFuture<CopycatClient> future = newSession().open().thenApply(s -> this);
    future.whenComplete((result, error) -> {
      if (error != null) {
        setState(State.CLOSED);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    if (state == State.CLOSED)
      return CompletableFuture.completedFuture(null);

    if (changeListener != null)
      changeListener.close();

    // Close the child session and call close listeners once complete.
    return session.close().whenComplete((result, error) -> setState(State.CLOSED));
  }

  @Override
  public boolean isClosed() {
    return state == State.CLOSED;
  }

  /**
   * A completable future related to a single operation.
   */
  private static final class OperationFuture<T> extends CompletableFuture<T> {
    private final Operation<T> operation;

    private OperationFuture(Operation<T> operation) {
      this.operation = operation;
    }
  }

  /**
   * State change listener.
   */
  private final class StateChangeListener implements Listener<State> {
    private final Consumer<State> callback;

    protected StateChangeListener(Consumer<State> callback) {
      this.callback = callback;
      changeListeners.add(this);
    }

    @Override
    public void accept(State state) {
      context.executor().execute(() -> callback.accept(state));
    }

    @Override
    public void close() {
      changeListeners.remove(this);
    }
  }

  /**
   * Event listener wrapper.
   */
  private final class EventListener<T> implements Listener<T> {
    private final String event;
    private final Consumer<T> callback;
    private Listener<T> parent;

    private EventListener(String event, Consumer<T> callback) {
      this.event = event;
      this.callback = callback;
      eventListeners.add(this);
    }

    /**
     * Registers the session event listener.
     */
    public void register() {
      if (ThreadContext.currentContext() == context) {
        parent = session.onEvent(event, callback);
      } else {
        context.execute(() -> {
          parent = session.onEvent(event, callback);
        }).join();
      }
    }

    @Override
    public void accept(T message) {
      context.executor().execute(() -> callback.accept(message));
    }

    @Override
    public void close() {
      parent.close();
      eventListeners.remove(this);
    }
  }

}
