/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.copycat.client.session.impl;

import io.atomix.catalyst.concurrent.Futures;
import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.concurrent.ThreadPoolContext;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.CommunicationStrategies;
import io.atomix.copycat.client.CommunicationStrategy;
import io.atomix.copycat.client.ConnectionStrategy;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.impl.CopycatClientState;
import io.atomix.copycat.client.session.CopycatSession;
import io.atomix.copycat.client.util.AddressSelectorManager;
import io.atomix.copycat.client.util.ClientConnectionManager;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.error.UnknownSessionException;
import io.atomix.copycat.protocol.*;
import io.atomix.copycat.session.ClosedSessionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.ConnectException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Client session manager.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class CopycatSessionManager {
  private static final Logger LOG = LoggerFactory.getLogger(CopycatSessionManager.class);
  private final CopycatClientState clientState;
  private final ClientConnectionManager connectionManager;
  private final CopycatConnection connection;
  private final ThreadContext threadContext;
  private final ScheduledExecutorService threadPoolExecutor;
  private final AddressSelectorManager selectorManager = new AddressSelectorManager();
  private final ConnectionStrategy strategy;
  private final Duration sessionTimeout;
  private final Duration unstableTimeout;
  private final Map<Long, CopycatSessionState> sessions = new ConcurrentHashMap<>();
  private volatile State state = State.CLOSED;
  private volatile Long unstableTime;
  private Duration interval;
  private Scheduled keepAlive;

  public CopycatSessionManager(CopycatClientState clientState, ClientConnectionManager connectionManager, ThreadContext threadContext, ScheduledExecutorService threadPoolExecutor, ConnectionStrategy connectionStrategy, Duration sessionTimeout, Duration unstableTimeout) {
    this.clientState = Assert.notNull(clientState, "clientState");
    this.connectionManager = Assert.notNull(connectionManager, "connectionManager");
    this.connection = new CopycatClientConnection(connectionManager, selectorManager.createSelector(CommunicationStrategies.ANY));
    this.threadContext = Assert.notNull(threadContext, "threadContext");
    this.threadPoolExecutor = Assert.notNull(threadPoolExecutor, "threadPoolExecutor");
    this.strategy = Assert.notNull(connectionStrategy, "connectionStrategy");
    this.sessionTimeout = Assert.notNull(sessionTimeout, "sessionTimeout");
    this.unstableTimeout = Assert.notNull(unstableTimeout, "unstableTimeout");
  }

  /**
   * Resets the session manager's cluster information.
   */
  public void resetConnections() {
    selectorManager.resetAll();
  }

  /**
   * Resets the session manager's cluster information.
   *
   * @param leader The leader address.
   * @param servers The collection of servers.
   */
  public void resetConnections(Address leader, Collection<Address> servers) {
    selectorManager.resetAll(leader, servers);
  }

  /**
   * Sets the session manager state.
   *
   * @param state The session manager state.
   */
  private void setState(State state) {
    this.state = state;
    switch (state) {
      case OPEN:
        clientState.setState(CopycatClient.State.CONNECTED);
        break;
      case UNSTABLE:
        clientState.setState(CopycatClient.State.SUSPENDED);
        if (unstableTime == null) {
          unstableTime = System.currentTimeMillis();
        } else if (System.currentTimeMillis() - unstableTime > unstableTimeout.toMillis()) {
          setState(State.EXPIRED);
        }
        break;
      case EXPIRED:
        clientState.setState(CopycatClient.State.CLOSED);
        sessions.values().forEach(CopycatSessionState::close);
        break;
      case CLOSED:
        clientState.setState(CopycatClient.State.CLOSED);
        sessions.values().forEach(CopycatSessionState::close);
        break;
    }
  }

  /**
   * Opens the session manager.
   *
   * @return A completable future to be called once the session manager is opened.
   */
  public CompletableFuture<Void> open() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    threadContext.execute(() -> connection.open().whenComplete((result, error) -> {
      if (error == null) {
        registerClient(new RegisterAttempt(1, future));
      } else {
        future.completeExceptionally(error);
      }
    }));
    return future;
  }

  /**
   * Opens a new session.
   *
   * @param name The session name.
   * @param type The session type.
   * @param communicationStrategy The strategy with which to communicate with servers.
   * @return A completable future to be completed once the session has been opened.
   */
  public CompletableFuture<CopycatSession> openSession(String name, String type, CommunicationStrategy communicationStrategy) {
    LOG.trace("Opening session; name: {}, type: {}", name, type);
    OpenSessionRequest request = OpenSessionRequest.builder()
      .withClient(clientState.getId())
      .withType(type)
      .withName(name)
      .build();

    LOG.trace("Sending {}", request);
    CompletableFuture<CopycatSession> future = new CompletableFuture<>();
    ThreadContext threadContext = new ThreadPoolContext(threadPoolExecutor, this.threadContext.serializer().clone());
    Runnable callback = () -> connection.<OpenSessionRequest, OpenSessionResponse>sendAndReceive(OpenSessionRequest.NAME, request).whenCompleteAsync((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          CopycatSessionState state = new CopycatSessionState(response.session(), name, type);
          sessions.put(state.getSessionId(), state);
          CopycatConnection leaderConnection = new CopycatLeaderConnection(state, connectionManager, selectorManager.createSelector(CommunicationStrategies.LEADER));
          CopycatConnection sessionConnection = new CopycatSessionConnection(state, connectionManager, selectorManager.createSelector(communicationStrategy), threadContext);
          leaderConnection.open().thenCompose(v -> sessionConnection.open()).whenComplete((connectResult, connectError) -> {
            if (connectError == null) {
              future.complete(new DefaultCopycatSession(state, leaderConnection, sessionConnection, threadContext, this));
            } else {
              future.completeExceptionally(connectError);
            }
          });
        } else {
          future.completeExceptionally(response.error().createException());
        }
      } else {
        future.completeExceptionally(error);
      }
    }, threadContext);

    if (threadContext.isCurrentContext()) {
      callback.run();
    } else {
      threadContext.execute(callback);
    }
    return future;
  }

  /**
   * Closes a session.
   *
   * @param sessionId The session identifier.
   * @return A completable future to be completed once the session is closed.
   */
  public CompletableFuture<Void> closeSession(long sessionId) {
    CopycatSessionState state = sessions.get(sessionId);
    if (state == null) {
      return Futures.exceptionalFuture(new UnknownSessionException("Unknown session: " + sessionId));
    }

    LOG.trace("Closing session {}", sessionId);
    CloseSessionRequest request = CloseSessionRequest.builder()
      .withSession(sessionId)
      .build();

    LOG.trace("Sending {}", request);
    CompletableFuture<Void> future = new CompletableFuture<>();
    Runnable callback = () -> connection.<CloseSessionRequest, CloseSessionResponse>sendAndReceive(CloseSessionRequest.NAME, request).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          sessions.remove(sessionId);
          future.complete(null);
        } else {
          future.completeExceptionally(response.error().createException());
        }
      } else {
        future.completeExceptionally(error);
      }
    });

    if (threadContext.isCurrentContext()) {
      callback.run();
    } else {
      threadContext.execute(callback);
    }
    return future;
  }

  /**
   * Expires the manager.
   *
   * @return A completable future to be completed once the session has been expired.
   */
  CompletableFuture<Void> expireSessions() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    threadContext.execute(() -> {
      if (keepAlive != null)
        keepAlive.cancel();
      setState(State.EXPIRED);
      future.complete(null);
    });
    return future;
  }

  /**
   * Registers a session.
   */
  private void registerClient(RegisterAttempt attempt) {
    LOG.debug("Registering client: attempt {}", attempt.attempt);

    RegisterRequest request = RegisterRequest.builder()
      .withClient(clientState.getUuid())
      .withTimeout(sessionTimeout.toMillis())
      .build();

    LOG.trace("{} - Sending {}", clientState.getUuid(), request);
    selectorManager.resetAll();
    connection.<RegisterRequest, RegisterResponse>sendAndReceive(RegisterRequest.NAME, request).whenComplete((response, error) -> {
      if (error == null) {
        LOG.trace("{} - Received {}", clientState.getUuid(), response);
        if (response.status() == Response.Status.OK) {
          clientState.setId(response.clientId());
          interval = Duration.ofMillis(response.timeout()).dividedBy(2);
          selectorManager.resetAll(response.leader(), response.members());
          setState(State.OPEN);
          LOG.info("{} - Registered client {}", clientState.getUuid(), clientState.getId());
          attempt.complete();
          keepAliveSessions();
        } else {
          strategy.attemptFailed(attempt);
        }
      } else {
        strategy.attemptFailed(attempt);
      }
    });
  }

  /**
   * Resets indexes for the given session.
   *
   * @param sessionId The session for which to reset indexes.
   * @return A completable future to be completed once the session's indexes have been reset.
   */
  CompletableFuture<Void> resetIndexes(long sessionId) {
    CopycatSessionState sessionState = sessions.get(sessionId);
    if (sessionState == null) {
      return Futures.exceptionalFuture(new IllegalArgumentException("Unknown session: " + sessionId));
    }

    CompletableFuture<Void> future = new CompletableFuture<>();

    KeepAliveRequest request = KeepAliveRequest.builder()
      .withClient(clientState.getId())
      .withSessionIds(new long[]{sessionId})
      .withCommandSequences(new long[]{sessionState.getCommandResponse()})
      .withEventIndexes(new long[]{sessionState.getEventIndex()})
      .withConnections(new long[]{sessionState.getConnection()})
      .build();

    LOG.trace("{} - Sending {}", clientState.getUuid(), request);
    connection.<KeepAliveRequest, KeepAliveResponse>sendAndReceive(KeepAliveRequest.NAME, request).whenComplete((response, error) -> {
        if (error == null) {
          LOG.trace("{} - Received {}", clientState.getUuid(), response);
          if (response.status() == Response.Status.OK) {
            future.complete(null);
          } else {
            future.completeExceptionally(response.error().createException());
          }
        } else {
          future.completeExceptionally(error);
        }
    });
    return future;
  }

  /**
   * Sends a keep-alive request to the cluster.
   */
  private void keepAliveSessions() {
    keepAliveSessions(true);
  }

  /**
   * Sends a keep-alive request to the cluster.
   */
  private void keepAliveSessions(boolean retryOnFailure) {
    // If the current sessions state is unstable, reset the connection before sending a keep-alive.
    if (state == State.UNSTABLE) {
      selectorManager.resetAll();
    }

    Map<Long, CopycatSessionState> sessions = new HashMap<>(this.sessions);
    long[] sessionIds = new long[sessions.size()];
    long[] commandResponses = new long[sessions.size()];
    long[] eventIndexes = new long[sessions.size()];
    long[] connections = new long[sessions.size()];

    int i = 0;
    for (CopycatSessionState sessionState : sessions.values()) {
      sessionIds[i] = sessionState.getSessionId();
      commandResponses[i] = sessionState.getCommandResponse();
      eventIndexes[i] = sessionState.getEventIndex();
      connections[i] = sessionState.getConnection();
      i++;
    }

    KeepAliveRequest request = KeepAliveRequest.builder()
      .withClient(clientState.getId())
      .withSessionIds(sessionIds)
      .withCommandSequences(commandResponses)
      .withEventIndexes(eventIndexes)
      .withConnections(connections)
      .build();

    LOG.trace("{} - Sending {}", clientState.getUuid(), request);
    connection.<KeepAliveRequest, KeepAliveResponse>sendAndReceive(KeepAliveRequest.NAME, request).whenComplete((response, error) -> {
      if (state != State.CLOSED) {
        if (error == null) {
          LOG.trace("{} - Received {}", clientState.getUuid(), response);
          // If the request was successful, update the address selector and schedule the next keep-alive.
          if (response.status() == Response.Status.OK) {
            selectorManager.resetAll(response.leader(), response.members());
            setState(State.OPEN);
            scheduleKeepAlive();
          }
          // If the session is unknown, immediate expire the session.
          else if (response.error() == CopycatError.Type.UNKNOWN_SESSION_ERROR) {
            setState(State.EXPIRED);
          }
          // If a leader is still set in the address selector, unset the leader and attempt to send another keep-alive.
          // This will ensure that the address selector selects all servers without filtering on the leader.
          else if (retryOnFailure && connection.leader() != null) {
            selectorManager.resetAll(null, connection.servers());
            keepAliveSessions(false);
          }
          // If no leader was set, set the session state to unstable and schedule another keep-alive.
          else {
            setState(State.UNSTABLE);
            scheduleKeepAlive();
          }
        }
        // If a leader is still set in the address selector, unset the leader and attempt to send another keep-alive.
        // This will ensure that the address selector selects all servers without filtering on the leader.
        else if (retryOnFailure && connection.leader() != null) {
          selectorManager.resetAll(null, connection.servers());
          keepAliveSessions(false);
        }
        // If no leader was set, set the session state to unstable and schedule another keep-alive.
        else {
          setState(State.UNSTABLE);
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
    keepAlive = threadContext.schedule(interval, () -> {
      keepAlive = null;
      if (state.isActive()) {
        keepAliveSessions();
      }
    });
  }

  /**
   * Closes the session manager.
   *
   * @return A completable future to be completed once the session manager is closed.
   */
  public CompletableFuture<Void> close() {
    if (state == State.EXPIRED)
      return CompletableFuture.completedFuture(null);

    CompletableFuture<Void> future = new CompletableFuture<>();
    threadContext.execute(() -> {
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
    // If the session is already closed, skip the unregister attempt.
    if (state == State.CLOSED) {
      future.complete(null);
      return;
    }

    LOG.debug("{} - Unregistering client: {}", clientState.getUuid(), clientState.getId());

    // If a keep-alive request is already pending, cancel it.
    if (keepAlive != null) {
      keepAlive.cancel();
      keepAlive = null;
    }

    // If the current sessions state is unstable, reset the connection before sending an unregister request.
    if (state == State.UNSTABLE) {
      selectorManager.resetAll();
    }

    UnregisterRequest request = UnregisterRequest.builder()
      .withClient(clientState.getId())
      .build();

    LOG.trace("{} - Sending {}", clientState, request);
    connection.<UnregisterRequest, UnregisterResponse>sendAndReceive(UnregisterRequest.NAME, request).whenComplete((response, error) -> {
      if (state != State.CLOSED) {
        if (error == null) {
          LOG.trace("{} - Received {}", clientState.getUuid(), response);
          // If the request was successful, update the session state and complete the close future.
          if (response.status() == Response.Status.OK) {
            setState(State.CLOSED);
            future.complete(null);
          }
          // If the session is unknown, immediate expire the session and complete the close future.
          else if (response.error() == CopycatError.Type.UNKNOWN_SESSION_ERROR) {
            setState(State.EXPIRED);
            future.complete(null);
          }
          // If a leader is still set in the address selector, unset the leader and send another unregister attempt.
          // This will ensure that the address selector selects all servers without filtering on the leader.
          else if (retryOnFailure && connection.leader() != null) {
            selectorManager.resetAll(null, connection.servers());
            unregister(false, future);
          }
          // If no leader was set, set the session state to unstable and fail the unregister attempt.
          else {
            setState(State.UNSTABLE);
            future.completeExceptionally(new ClosedSessionException("failed to unregister session"));
          }
        }
        // If a leader is still set in the address selector, unset the leader and send another unregister attempt.
        // This will ensure that the address selector selects all servers without filtering on the leader.
        else if (retryOnFailure && connection.leader() != null) {
          selectorManager.resetAll(null, connection.servers());
          unregister(false, future);
        }
        // If no leader was set, set the session state to unstable and schedule another unregister attempt.
        else {
          setState(State.UNSTABLE);
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
      setState(State.CLOSED);
    }, threadContext);
  }

  @Override
  public String toString() {
    return String.format("%s[client=%s]", getClass().getSimpleName(), clientState.getUuid());
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
      LOG.debug("Retrying session register attempt");
      registerClient(new RegisterAttempt(attempt + 1, future));
    }

    @Override
    public void retry(Duration after) {
      LOG.debug("Retrying session register attempt");
      threadContext.schedule(after, () -> registerClient(new RegisterAttempt(attempt + 1, future)));
    }
  }

  /**
   * Session manager state.
   */
  private enum State {
    OPEN(true),
    UNSTABLE(true),
    EXPIRED(false),
    CLOSED(false);

    private final boolean active;

    State(boolean active) {
      this.active = active;
    }

    /**
     * Returns whether the state is active, requiring keep-alives.
     *
     * @return Whether the state is active.
     */
    public boolean isActive() {
      return active;
    }
  }

}
