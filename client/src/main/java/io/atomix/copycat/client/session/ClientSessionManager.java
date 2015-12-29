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

import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.concurrent.Scheduled;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.ConnectionStrategy;
import io.atomix.copycat.client.request.KeepAliveRequest;
import io.atomix.copycat.client.request.RegisterRequest;
import io.atomix.copycat.client.request.UnregisterRequest;
import io.atomix.copycat.client.response.KeepAliveResponse;
import io.atomix.copycat.client.response.RegisterResponse;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.client.response.UnregisterResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * Client session manager.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class ClientSessionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientSessionManager.class);
  private static final int MAX_FAILURES = 1;
  private final ClientSessionState state;
  private final Connection connection;
  private final ThreadContext context;
  private final ConnectionStrategy strategy;
  private final Set<Listener<Session.State>> changeListeners = new CopyOnWriteArraySet<>();
  private Duration interval;
  private Scheduled keepAlive;
  private int failures;

  ClientSessionManager(Connection connection, ClientSessionState state, ThreadContext context, ConnectionStrategy connectionStrategy) {
    this.connection = Assert.notNull(connection, "connection");
    this.state = Assert.notNull(state, "state");
    this.context = Assert.notNull(context, "context");
    this.strategy = Assert.notNull(connectionStrategy, "connectionStrategy");
  }

  /**
   * Opens the session manager.
   *
   * @return A completable future to be called once the session manager is opened.
   */
  public CompletableFuture<Void> open() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    register(new RegisterAttempt(1, 0, future));
    return future;
  }

  /**
   * Registers a session.
   */
  private void register(RegisterAttempt attempt) {
    RegisterRequest request = RegisterRequest.builder()
      .withClient(state.getClientId())
      .withSession(attempt.session)
      .build();
    connection.<RegisterRequest, RegisterResponse>send(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          interval = Duration.ofMillis(response.timeout()).dividedBy(2);
          state.setSessionId(response.session())
            .setEventIndex(response.session())
            .setCompleteIndex(response.session())
            .setState(Session.State.OPEN);
          attempt.complete();
          keepAlive();
        } else {
          strategy.attemptFailed(attempt);
        }
      } else {
        strategy.attemptFailed(attempt);
      }
    });
  }

  /**
   * Sends a keep-alive request to the cluster.
   */
  private void keepAlive() {
    long sessionId = state.getSessionId();
    KeepAliveRequest request = KeepAliveRequest.builder()
      .withSession(sessionId)
      .withCommandSequence(state.getCommandResponse())
      .withEventIndex(state.getCompleteIndex())
      .build();
    connection.<KeepAliveRequest, KeepAliveResponse>send(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          failures = 0;
          keepAlive = context.schedule(interval, this::keepAlive);
        } else if (failures == MAX_FAILURES) {
          state.setState(Session.State.EXPIRED);
        } else {
          failures++;
          keepAlive = context.schedule(interval, this::keepAlive);
        }
      } else if (failures == MAX_FAILURES) {
        state.setState(Session.State.EXPIRED);
      } else {
        failures++;
        keepAlive = context.schedule(interval, this::keepAlive);
      }
    });
  }

  /**
   * Closes the session manager.
   *
   * @return A completable future to be completed once the session manager is closed.
   */
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    keepAlive.cancel();
    state.setState(Session.State.CLOSED);
    unregister(future);
    return future;
  }

  /**
   * Unregisters the session.
   *
   * @param future A completable future to be completed once the session is unregistered.
   */
  private void unregister(CompletableFuture<Void> future) {
    // If a keep-alive request is already pending, cancel it.
    if (keepAlive != null)
      keepAlive.cancel();

    long sessionId = state.getSessionId();
    UnregisterRequest request = UnregisterRequest.builder()
      .withSession(sessionId)
      .build();
    connection.<UnregisterRequest, UnregisterResponse>send(request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          state.setState(Session.State.CLOSED);
          future.complete(null);
        } else if (failures == MAX_FAILURES) {
          state.setState(Session.State.EXPIRED);
          future.completeExceptionally(new IllegalStateException("failed to unregister session"));
        } else {
          failures++;
          keepAlive = context.schedule(interval, () -> unregister(future));
        }
      } else if (failures == MAX_FAILURES) {
        state.setState(Session.State.EXPIRED);
        future.completeExceptionally(new IllegalStateException("failed to unregister session"));
      } else {
        failures++;
        keepAlive = context.schedule(interval, () -> unregister(future));
      }
    });
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d]", getClass().getSimpleName(), state.getSessionId());
  }

  /**
   * Client session connection attempt.
   */
  private final class RegisterAttempt implements ConnectionStrategy.Attempt {
    private final int attempt;
    private final long session;
    private final CompletableFuture<Void> future;

    private RegisterAttempt(int attempt, long session, CompletableFuture<Void> future) {
      this.attempt = attempt;
      this.session = session;
      this.future = future;
    }

    @Override
    public int attempt() {
      return attempt;
    }

    /**
     * Completes the attempt successfully.
     */
    public void complete() {
      complete(null);
    }

    /**
     * Completes the attempt successfully.
     *
     * @param result The attempt result.
     */
    public void complete(Void result) {
      future.complete(result);
    }

    @Override
    public void fail() {
      future.completeExceptionally(new ConnectException("failed to register session"));
    }

    @Override
    public void fail(Throwable error) {
      future.completeExceptionally(error);
    }

    @Override
    public void retry() {
      LOGGER.debug("Retrying session register attempt");
      register(new RegisterAttempt(attempt + 1, session, future));
    }

    @Override
    public void retry(Duration after) {
      LOGGER.debug("Retrying session register attempt");
      context.schedule(after, () -> register(new RegisterAttempt(attempt + 1, session, future)));
    }
  }

}
