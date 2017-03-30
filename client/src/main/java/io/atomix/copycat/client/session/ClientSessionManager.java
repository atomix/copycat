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

import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.ConnectionStrategy;
import io.atomix.copycat.client.util.ClientConnection;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.*;
import io.atomix.copycat.session.ClosedSessionException;
import io.atomix.copycat.session.Session;

import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Client session manager.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class ClientSessionManager {
  private final ClientSessionState state;
  private final ClientConnection connection;
  private final ThreadContext context;
  private final ConnectionStrategy strategy;
  private final Duration sessionTimeout;
  private Duration interval;
  private Scheduled keepAlive;

  ClientSessionManager(ClientConnection connection, ClientSessionState state, ThreadContext context, ConnectionStrategy connectionStrategy, Duration sessionTimeout) {
    this.connection = Assert.notNull(connection, "connection");
    this.state = Assert.notNull(state, "state");
    this.context = Assert.notNull(context, "context");
    this.strategy = Assert.notNull(connectionStrategy, "connectionStrategy");
    this.sessionTimeout = Assert.notNull(sessionTimeout, "sessionTimeout");
  }

  /**
   * Opens the session manager.
   *
   * @return A completable future to be called once the session manager is opened.
   */
  public CompletableFuture<Void> open() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.executor().execute(() -> register(new RegisterAttempt(1, future)));
    return future;
  }

  /**
   * Expires the manager.
   *
   * @return A completable future to be completed once the session has been expired.
   */
  public CompletableFuture<Void> expire() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      if (keepAlive != null)
        keepAlive.cancel();
      state.setState(Session.State.EXPIRED);
      future.complete(null);
    });
    return future;
  }

  /**
   * Registers a session.
   */
  private void register(RegisterAttempt attempt) {
    state.getLogger().debug("Registering session: attempt {}", attempt.attempt);

    RegisterRequest request = RegisterRequest.builder()
      .withClient(state.getClientId())
      .withTimeout(sessionTimeout.toMillis())
      .build();

    state.getLogger().trace("Sending {}", request);
    connection.reset().<RegisterRequest, RegisterResponse>send(request).whenComplete((response, error) -> {
      if (error == null) {
        state.getLogger().trace("Received {}", response);
        if (response.status() == Response.Status.OK) {
          interval = Duration.ofMillis(response.timeout()).dividedBy(2);
          connection.reset(response.leader(), response.members());
          state.setSessionId(response.session())
            .setState(Session.State.OPEN);
          state.getLogger().info("Registered session {}", response.session());
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
    keepAlive(true);
  }

  /**
   * Sends a keep-alive request to the cluster.
   */
  private void keepAlive(boolean retryOnFailure) {
    long sessionId = state.getSessionId();

    // If the current sessions state is unstable, reset the connection before sending a keep-alive.
    if (state.getState() == Session.State.UNSTABLE)
      connection.reset();

    KeepAliveRequest request = KeepAliveRequest.builder()
      .withSession(sessionId)
      .withCommandSequence(state.getCommandResponse())
      .withEventIndex(state.getEventIndex())
      .build();

    state.getLogger().trace("{} - Sending {}", sessionId, request);
    connection.<KeepAliveRequest, KeepAliveResponse>send(request).whenComplete((response, error) -> {
      if (state.getState() != Session.State.CLOSED) {
        if (error == null) {
          state.getLogger().trace("{} - Received {}", sessionId, response);
          // If the request was successful, update the address selector and schedule the next keep-alive.
          if (response.status() == Response.Status.OK) {
            connection.reset(response.leader(), response.members());
            state.setState(Session.State.OPEN);
            scheduleKeepAlive();
          }
          // If the session is unknown, immediate expire the session.
          else if (response.error() == CopycatError.Type.UNKNOWN_SESSION_ERROR) {
            state.setState(Session.State.EXPIRED);
          }
          // If a leader is still set in the address selector, unset the leader and attempt to send another keep-alive.
          // This will ensure that the address selector selects all servers without filtering on the leader.
          else if (retryOnFailure && connection.leader() != null) {
            connection.reset(null, connection.servers());
            keepAlive(false);
          }
          // If no leader was set, set the session state to unstable and schedule another keep-alive.
          else {
            state.setState(Session.State.UNSTABLE);
            scheduleKeepAlive();
          }
        }
        // If a leader is still set in the address selector, unset the leader and attempt to send another keep-alive.
        // This will ensure that the address selector selects all servers without filtering on the leader.
        else if (retryOnFailure && connection.leader() != null) {
          connection.reset(null, connection.servers());
          keepAlive(false);
        }
        // If no leader was set, set the session state to unstable and schedule another keep-alive.
        else {
          state.setState(Session.State.UNSTABLE);
          scheduleKeepAlive();
        }
      }
    });
  }

  /**
   * Schedules a keep-alive request.
   */
  private void scheduleKeepAlive() {
    if (keepAlive != null)
      keepAlive.cancel();
    keepAlive = context.schedule(interval, () -> {
      keepAlive = null;
      if (state.getState().active()) {
        keepAlive();
      }
    });
  }

  /**
   * Closes the session manager.
   *
   * @return A completable future to be completed once the session manager is closed.
   */
  public CompletableFuture<Void> close() {
    if (state.getState() == Session.State.EXPIRED)
      return CompletableFuture.completedFuture(null);

    CompletableFuture<Void> future = new CompletableFuture<>();
    context.executor().execute(() -> {
      if (keepAlive != null) {
        keepAlive.cancel();
        keepAlive = null;
      }
      unregister(future);
    });
    return future;
  }

  /**
   * Unregisters the session.
   */
  private void unregister(CompletableFuture<Void> future) {
    unregister(true, future);
  }

  /**
   * Unregisters the session.
   *
   * @param future A completable future to be completed once the session is unregistered.
   */
  private void unregister(boolean retryOnFailure, CompletableFuture<Void> future) {
    long sessionId = state.getSessionId();

    // If the session is already closed, skip the unregister attempt.
    if (state.getState() == Session.State.CLOSED) {
      future.complete(null);
      return;
    }

    state.getLogger().debug("Unregistering session: {}", sessionId);

    // If a keep-alive request is already pending, cancel it.
    if (keepAlive != null) {
      keepAlive.cancel();
      keepAlive = null;
    }

    // If the current sessions state is unstable, reset the connection before sending an unregister request.
    if (state.getState() == Session.State.UNSTABLE) {
      connection.reset();
    }

    UnregisterRequest request = UnregisterRequest.builder()
      .withSession(sessionId)
      .build();

    state.getLogger().trace("{} - Sending {}", sessionId, request);
    connection.<UnregisterRequest, UnregisterResponse>send(request).whenComplete((response, error) -> {
      if (state.getState() != Session.State.CLOSED) {
        if (error == null) {
          state.getLogger().trace("{} - Received {}", sessionId, response);
          // If the request was successful, update the session state and complete the close future.
          if (response.status() == Response.Status.OK) {
            state.setState(Session.State.CLOSED);
            future.complete(null);
          }
          // If the session is unknown, immediate expire the session and complete the close future.
          else if (response.error() == CopycatError.Type.UNKNOWN_SESSION_ERROR) {
            state.setState(Session.State.EXPIRED);
            future.complete(null);
          }
          // If a leader is still set in the address selector, unset the leader and send another unregister attempt.
          // This will ensure that the address selector selects all servers without filtering on the leader.
          else if (retryOnFailure && connection.leader() != null) {
            connection.reset(null, connection.servers());
            unregister(false, future);
          }
          // If no leader was set, set the session state to unstable and fail the unregister attempt.
          else {
            state.setState(Session.State.UNSTABLE);
            future.completeExceptionally(new ClosedSessionException("failed to unregister session"));
          }
        }
        // If a leader is still set in the address selector, unset the leader and send another unregister attempt.
        // This will ensure that the address selector selects all servers without filtering on the leader.
        else if (retryOnFailure && connection.leader() != null) {
          connection.reset(null, connection.servers());
          unregister(false, future);
        }
        // If no leader was set, set the session state to unstable and schedule another unregister attempt.
        else {
          state.setState(Session.State.UNSTABLE);
          future.completeExceptionally(new ClosedSessionException("failed to unregister session"));
        }
      }
    });
  }

  /**
   * Kills the client session manager.
   *
   * @return A completable future to be completed once the session manager is killed.
   */
  public CompletableFuture<Void> kill() {
    return CompletableFuture.runAsync(() -> {
      if (keepAlive != null)
        keepAlive.cancel();
      state.setState(Session.State.CLOSED);
    }, context.executor());
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
    private final CompletableFuture<Void> future;

    private RegisterAttempt(int attempt, CompletableFuture<Void> future) {
      this.attempt = attempt;
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
      state.getLogger().debug("Retrying session register attempt");
      register(new RegisterAttempt(attempt + 1, future));
    }

    @Override
    public void retry(Duration after) {
      state.getLogger().debug("Retrying session register attempt");
      context.schedule(after, () -> register(new RegisterAttempt(attempt + 1, future)));
    }
  }

}
