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
import io.atomix.copycat.client.session.CopycatSession;
import io.atomix.copycat.client.util.AddressSelectorManager;
import io.atomix.copycat.client.util.ClientConnectionManager;
import io.atomix.copycat.error.UnknownSessionException;
import io.atomix.copycat.protocol.CloseSessionRequest;
import io.atomix.copycat.protocol.CloseSessionResponse;
import io.atomix.copycat.protocol.KeepAliveRequest;
import io.atomix.copycat.protocol.KeepAliveResponse;
import io.atomix.copycat.protocol.OpenSessionRequest;
import io.atomix.copycat.protocol.OpenSessionResponse;
import io.atomix.copycat.protocol.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Client session manager.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class CopycatSessionManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(CopycatSessionManager.class);
  private final String clientId;
  private final ClientConnectionManager connectionManager;
  private final CopycatConnection connection;
  private final ThreadContext threadContext;
  private final ScheduledExecutorService threadPoolExecutor;
  private final AddressSelectorManager selectorManager;
  private final Map<Long, CopycatSessionState> sessions = new ConcurrentHashMap<>();
  private final AtomicBoolean open = new AtomicBoolean();
  private Scheduled keepAlive;

  public CopycatSessionManager(String clientId, ClientConnectionManager connectionManager, AddressSelectorManager selectorManager, ThreadContext threadContext, ScheduledExecutorService threadPoolExecutor) {
    this.clientId = Assert.notNull(clientId, "clientId");
    this.connectionManager = Assert.notNull(connectionManager, "connectionManager");
    this.selectorManager = Assert.notNull(selectorManager, "selectorManager");
    this.connection = new CopycatClientConnection(clientId, connectionManager, selectorManager.createSelector(CommunicationStrategies.ANY));
    this.threadContext = Assert.notNull(threadContext, "threadContext");
    this.threadPoolExecutor = Assert.notNull(threadPoolExecutor, "threadPoolExecutor");
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
   * Opens the session manager.
   *
   * @return A completable future to be called once the session manager is opened.
   */
  public CompletableFuture<Void> open() {
    if (open.compareAndSet(false, true)) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      threadContext.execute(() -> {
        connection.open().whenComplete((result, error) -> {
          if (error == null) {
            future.complete(null);
          } else {
            future.completeExceptionally(error);
          }
        });
      });
      return future;
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Opens a new session.
   *
   * @param name The session name.
   * @param type The session type.
   * @param communicationStrategy The strategy with which to communicate with servers.
   * @param timeout The session timeout.
   * @return A completable future to be completed once the session has been opened.
   */
  public CompletableFuture<CopycatSession> openSession(String name, String type, CommunicationStrategy communicationStrategy, Duration timeout) {
    LOGGER.trace("Opening session; name: {}, type: {}", name, type);
    OpenSessionRequest request = OpenSessionRequest.builder()
      .withType(type)
      .withName(name)
      .withTimeout(timeout.toMillis())
      .build();

    LOGGER.trace("{} - Sending {}", clientId, request);
    CompletableFuture<CopycatSession> future = new CompletableFuture<>();
    ThreadContext threadContext = new ThreadPoolContext(threadPoolExecutor, this.threadContext.serializer().clone());
    Runnable callback = () -> connection.<OpenSessionRequest, OpenSessionResponse>sendAndReceive(OpenSessionRequest.NAME, request).whenCompleteAsync((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          CopycatSessionState state = new CopycatSessionState(response.session(), name, type, response.timeout());
          sessions.put(state.getSessionId(), state);
          resetIndexes(state.getSessionId());
          CopycatConnection leaderConnection = new CopycatLeaderConnection(state, connectionManager, selectorManager.createSelector(CommunicationStrategies.LEADER));
          CopycatConnection sessionConnection = new CopycatSessionConnection(state, connectionManager, selectorManager.createSelector(communicationStrategy), threadContext);
          leaderConnection.open().thenCompose(v -> sessionConnection.open()).whenComplete((connectResult, connectError) -> {
            if (connectError == null) {
              keepAliveSessions();
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

    LOGGER.trace("Closing session {}", sessionId);
    CloseSessionRequest request = CloseSessionRequest.builder()
      .withSession(sessionId)
      .build();

    LOGGER.trace("Sending {}", request);
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
      if (keepAlive != null) {
        keepAlive.cancel();
      }
      for (CopycatSessionState session : sessions.values()) {
        session.setState(CopycatSession.State.CLOSED);
      }
      future.complete(null);
    });
    return future;
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
      .withSessionIds(new long[]{sessionId})
      .withCommandSequences(new long[]{sessionState.getCommandResponse()})
      .withEventIndexes(new long[]{sessionState.getEventIndex()})
      .withConnections(new long[]{sessionState.getConnection()})
      .build();

    LOGGER.trace("{} - Sending {}", clientId, request);
    connection.<KeepAliveRequest, KeepAliveResponse>sendAndReceive(KeepAliveRequest.NAME, request).whenComplete((response, error) -> {
        if (error == null) {
          LOGGER.trace("{} - Received {}", clientId, response);
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
      .withSessionIds(sessionIds)
      .withCommandSequences(commandResponses)
      .withEventIndexes(eventIndexes)
      .withConnections(connections)
      .build();

    LOGGER.trace("{} - Sending {}", clientId, request);
    connection.<KeepAliveRequest, KeepAliveResponse>sendAndReceive(KeepAliveRequest.NAME, request).whenComplete((response, error) -> {
      if (open.get()) {
        if (error == null) {
          LOGGER.trace("{} - Received {}", clientId, response);
          // If the request was successful, update the address selector and schedule the next keep-alive.
          if (response.status() == Response.Status.OK) {
            selectorManager.resetAll(response.leader(), response.members());
            sessions.values().forEach(s -> s.setState(CopycatSession.State.CONNECTED));
            scheduleKeepAlive();
          }
          // If a leader is still set in the address selector, unset the leader and attempt to send another keep-alive.
          // This will ensure that the address selector selects all servers without filtering on the leader.
          else if (retryOnFailure && connection.leader() != null) {
            selectorManager.resetAll(null, connection.servers());
            keepAliveSessions(false);
          }
          // If no leader was set, set the session state to unstable and schedule another keep-alive.
          else {
            sessions.values().forEach(s -> s.setState(CopycatSession.State.SUSPENDED));
            selectorManager.resetAll();
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
          sessions.values().forEach(s -> s.setState(CopycatSession.State.SUSPENDED));
          selectorManager.resetAll();
          scheduleKeepAlive();
        }
      }
    });
  }

  /**
   * Schedules a keep-alive request.
   */
  private void scheduleKeepAlive() {
    OptionalLong minTimeout = sessions.values().stream().mapToLong(CopycatSessionState::getSessionTimeout).min();
    if (minTimeout.isPresent()) {
      if (keepAlive != null) {
        keepAlive.cancel();
      }
      keepAlive = threadContext.schedule(Duration.ofMillis(minTimeout.getAsLong()).dividedBy(2), () -> {
        keepAlive = null;
        if (open.get()) {
          keepAliveSessions();
        }
      });
    }
  }

  /**
   * Closes the session manager.
   *
   * @return A completable future to be completed once the session manager is closed.
   */
  public CompletableFuture<Void> close() {
    if (open.compareAndSet(true, false)) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      threadContext.execute(() -> {
        if (keepAlive != null) {
          keepAlive.cancel();
          keepAlive = null;
        }
        future.complete(null);
      });
      return future;
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Kills the client session manager.
   *
   * @return A completable future to be completed once the session manager is killed.
   */
  public CompletableFuture<Void> kill() {
    return CompletableFuture.runAsync(() -> {
      if (keepAlive != null) {
        keepAlive.cancel();
      }
    }, threadContext);
  }

  @Override
  public String toString() {
    return String.format("%s[client=%s]", getClass().getSimpleName(), clientId);
  }

}
