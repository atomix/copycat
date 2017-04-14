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

import io.atomix.catalyst.concurrent.BlockingFuture;
import io.atomix.catalyst.concurrent.Futures;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.session.ClientSession;
import io.atomix.copycat.client.util.AddressSelector;
import io.atomix.copycat.session.ClosedSessionException;
import io.atomix.copycat.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Default Copycat client implementation.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class DefaultCopycatClient implements CopycatClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultCopycatClient.class);
  private static final String DEFAULT_HOST = "0.0.0.0";
  private static final int DEFAULT_PORT = 8700;
  private final String clientId;
  private final Collection<Address> cluster;
  private final Transport transport;
  private final ThreadContext ioContext;
  private final ThreadContext eventContext;
  private final AddressSelector selector;
  private final Duration sessionTimeout;
  private final Duration unstabilityTimeout;
  private final ConnectionStrategy connectionStrategy;
  private final RecoveryStrategy recoveryStrategy;
  private ClientSession session;
  private volatile State state = State.CLOSED;
  private volatile CompletableFuture<CopycatClient> openFuture;
  private volatile CompletableFuture<CopycatClient> recoverFuture;
  private volatile CompletableFuture<Void> closeFuture;
  private final Set<StateChangeListener> changeListeners = new CopyOnWriteArraySet<>();
  private final Set<EventListener<?>> eventListeners = new CopyOnWriteArraySet<>();
  private Listener<Session.State> changeListener;

  DefaultCopycatClient(String clientId, Collection<Address> cluster, Transport transport, ThreadContext ioContext, ThreadContext eventContext, ServerSelectionStrategy selectionStrategy, ConnectionStrategy connectionStrategy, RecoveryStrategy recoveryStrategy, Duration sessionTimeout, Duration unstabilityTimeout) {
    this.clientId = Assert.notNull(clientId, "clientId");
    this.cluster = Assert.notNull(cluster, "cluster");
    this.transport = Assert.notNull(transport, "transport");
    this.ioContext = Assert.notNull(ioContext, "ioContext");
    this.eventContext = Assert.notNull(eventContext, "eventContext");
    this.selector = new AddressSelector(selectionStrategy);
    this.connectionStrategy = Assert.notNull(connectionStrategy, "connectionStrategy");
    this.recoveryStrategy = Assert.notNull(recoveryStrategy, "recoveryStrategy");
    this.sessionTimeout = Assert.notNull(sessionTimeout, "sessionTimeout");
    this.unstabilityTimeout = Assert.notNull(unstabilityTimeout, "unstabilityTimeout");;
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
      LOGGER.debug("State changed: {}", state);
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
    return context != null ? context.serializer() : this.eventContext.serializer();
  }

  @Override
  public Session session() {
    return session;
  }

  @Override
  public ThreadContext context() {
    return eventContext;
  }

  /**
   * Creates a new child session.
   */
  private ClientSession newSession() {
    ClientSession session = new ClientSession(clientId, transport.client(), selector, ioContext, connectionStrategy, sessionTimeout,
                                              unstabilityTimeout
    );

    // Update the session change listener.
    if (changeListener != null)
      changeListener.close();
    changeListener = session.onStateChange(this::onStateChange);

    // Register all event listeners.
    eventListeners.forEach(l -> l.register(session));
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
      case STALE:
        setState(State.SUSPENDED);
        this.close();
        break;
      // When the session is expired, transition the state to SUSPENDED if necessary. The recovery strategy
      // must determine whether to attempt to recover the client.
      case EXPIRED:
        setState(State.SUSPENDED);
        recoveryStrategy.recover(this);
        break;
      case CLOSED:
        setState(State.CLOSED);
        break;
      default:
        break;
    }
  }

  @Override
  public synchronized CompletableFuture<CopycatClient> connect(Collection<Address> cluster) {
    if (state != State.CLOSED)
      return CompletableFuture.completedFuture(this);

    if (openFuture == null) {
      openFuture = new CompletableFuture<>();

      // If the provided cluster list is null or empty, use the default list.
      if (cluster == null || cluster.isEmpty()) {
        cluster = this.cluster;
      }

      // If the default list is null or empty, use the default host:port.
      if (cluster == null || cluster.isEmpty()) {
        cluster = Collections.singletonList(new Address(DEFAULT_HOST, DEFAULT_PORT));
      }

      // Reset the connection list to allow the selection strategy to prioritize connections.
      selector.reset(null, cluster);

      // Create and register a new session.
      session = newSession();
      session.register().whenCompleteAsync((result, error) -> {
        if (error == null) {
          openFuture.complete(this);
        } else {
          openFuture.completeExceptionally(error);
        }
      }, eventContext.executor());
    }
    return openFuture;
  }

  @Override
  public <T> CompletableFuture<T> submit(Command<T> command) {
    ClientSession session = this.session;
    if (session == null)
      return Futures.exceptionalFuture(new ClosedSessionException("session closed"));

    BlockingFuture<T> future = new BlockingFuture<>();
    session.submit(command).whenComplete((result, error) -> {
      if (eventContext.isBlocked()) {
        future.accept(result, error);
      } else {
        eventContext.executor().execute(() -> future.accept(result, error));
      }
    });
    return future;
  }

  @Override
  public <T> CompletableFuture<T> submit(Query<T> query) {
    ClientSession session = this.session;
    if (session == null)
      return Futures.exceptionalFuture(new ClosedSessionException("session closed"));

    BlockingFuture<T> future = new BlockingFuture<>();
    session.submit(query).whenComplete((result, error) -> {
      if (eventContext.isBlocked()) {
        future.accept(result, error);
      } else {
        eventContext.executor().execute(() -> future.accept(result, error));
      }
    });
    return future;
  }

  @Override
  public Listener<Void> onEvent(String event, Runnable callback) {
    return onEvent(event, v -> callback.run());
  }

  @Override
  public <T> Listener<T> onEvent(String event, Consumer<T> callback) {
    EventListener<T> listener = new EventListener<>(event, callback);
    listener.register(session);
    return listener;
  }

  @Override
  public synchronized CompletableFuture<CopycatClient> recover() {
    if (recoverFuture == null) {
      LOGGER.debug("Recovering session {}", this.session.id());
      recoverFuture = new CompletableFuture<>();
      session.close().whenCompleteAsync((closeResult, closeError) -> {
        session = newSession();
        session.register().whenCompleteAsync((registerResult, registerError) -> {
          CompletableFuture<CopycatClient> recoverFuture = this.recoverFuture;
          if (registerError == null) {
            recoverFuture.complete(this);
          } else {
            recoverFuture.completeExceptionally(registerError);
          }
          this.recoverFuture = null;
        }, eventContext.executor());
      }, eventContext.executor());
    }
    return recoverFuture;
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    if (state == State.CLOSED)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      // Close the child session and call close listeners once complete.
      closeFuture = new CompletableFuture<>();
      session.close().whenCompleteAsync((result, error) -> {
        setState(State.CLOSED);
        CompletableFuture.runAsync(() -> {
          ioContext.close();
          eventContext.close();
          transport.close();
          if (error == null) {
            closeFuture.complete(null);
          } else {
            closeFuture.completeExceptionally(error);
          }
        });
      }, eventContext.executor());
    }
    return closeFuture;
  }

  /**
   * Kills the client.
   *
   * @return A completable future to be completed once the client's session has been killed.
   */
  public synchronized CompletableFuture<Void> kill() {
    if (state == State.CLOSED)
      return CompletableFuture.completedFuture(null);

    if (closeFuture == null) {
      closeFuture = session.kill()
        .whenComplete((result, error) -> {
          setState(State.CLOSED);
          CompletableFuture.runAsync(() -> {
            ioContext.close();
            eventContext.close();
            transport.close();
          });
        });
    }
    return closeFuture;
  }

  @Override
  public int hashCode() {
    return 23 + 37 * (session != null ? session.hashCode() : 0);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof DefaultCopycatClient && ((DefaultCopycatClient) object).session() == session;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%s]", getClass().getSimpleName(), session);
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
      eventContext.executor().execute(() -> callback.accept(state));
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
    public void register(ClientSession session) {
      parent = session.onEvent(event, this);
    }

    @Override
    public void accept(T message) {
      if (eventContext.isBlocked()) {
        callback.accept(message);
      } else {
        eventContext.executor().execute(() -> callback.accept(message));
      }
    }

    @Override
    public void close() {
      parent.close();
      eventListeners.remove(this);
    }
  }
}
