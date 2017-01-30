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

import io.atomix.copycat.ConsistencyLevel;
import io.atomix.copycat.client.ConnectionStrategy;
import io.atomix.copycat.client.util.AddressSelector;
import io.atomix.copycat.client.util.ClientConnection;
import io.atomix.copycat.protocol.ProtocolClient;
import io.atomix.copycat.util.Assert;
import io.atomix.copycat.util.concurrent.Futures;
import io.atomix.copycat.util.concurrent.Listener;
import io.atomix.copycat.util.concurrent.ThreadContext;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Handles submitting state machine commands and queries to the Copycat cluster.
 * <p>
 * The client session is responsible for maintaining a client's connection to a Copycat cluster and coordinating
 * the submission of commands and queries to various nodes in the cluster. Client
 * sessions are single-use objects that represent the context within which a cluster can guarantee linearizable
 * semantics for state machine operations. When a session is {@link #register() opened}, the session will register
 * itself with the cluster by attempting to contact each of the known servers. Once the session has been successfully
 * registered, kee-alive requests will be periodically sent to keep the session alive.
 * <p>
 * Sessions are responsible for sequencing concurrent operations to ensure they're applied to the system state
 * in the order in which they were submitted by the client. To do so, the session coordinates with its server-side
 * counterpart using unique per-operation sequence numbers.
 * <p>
 * In the event that the client session expires, clients are responsible for opening a new session by creating and
 * opening a new session object.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ClientSession {

  /**
   * Represents the state of a session.
   * <p>
   * Throughout the lifetime of a session, the session may change state as a result of communication or the lack
   * thereof between the client and the cluster. See the specific states for more documentation.
   */
  public enum State {

    /**
     * Indicates that the session is connected and open.
     * <p>
     * This is the initial state of a session upon registration. Once the session is registered with the cluster,
     * the session's state will be {@code OPEN} and will remain open so long as the client is able to communicate
     * with the cluster to keep its session alive.
     * <p>
     * Clients withe a session in the {@code OPEN} state can be assumed to be operating normally and are guaranteed
     * to benefit from linearizable reads and writes.
     */
    OPEN(true),

    /**
     * Indicates that the session in an unstable state and may or may not be {@link #EXPIRED}.
     * <p>
     * The unstable state is indicative of a state in which a client is unable to communicate with the cluster and
     * therefore cannot determine whether its session is or is not expired. Until the client is able to re-establish
     * communication with the cluster, its session will remain in this state. While in the {@code UNSTABLE} state,
     * users of the client should assume that the session may have been expired and that other clients may react
     * to the client having been expired while in the unstable state.
     * <p>
     * Commands submitted or completed in the {@code UNSTABLE} state may not be linearizable. An unstable session
     * may be expired and a client may have to resubmit associated commands once a new session in registered. This
     * can lead to duplicate commands being applied to servers state machines, thus breaking linearizability.
     * <p>
     * Once the client is able to re-establish communication with the cluster again it will determine whether the
     * session is still active or indeed has been expired. The session will either transition back to the
     * {@link #OPEN} state or {@link #EXPIRED} based on feedback from the cluster. Only the cluster can explicitly
     * expire a session.
     */
    UNSTABLE(true),

    /**
     * Indicates that the session is expired.
     * <p>
     * Once an {@link #UNSTABLE} client re-establishes communication with the cluster, the cluster may indicate to
     * the client that its session has been expired. In that case, the client may no longer submit operations under
     * the expired session and must register a new session to continue operating on server state machines.
     * <p>
     * When a client's session is expired, commands submitted under the expired session that have not yet been completed
     * may or may not be applied to server state machines. Linearizability is guaranteed only within the context of a
     * session, and so linearizability may be broken if operations are resubmitted across sessions.
     */
    EXPIRED(false),

    /**
     * Indicates that the session has been closed.
     * <p>
     * This state indicates that the client's session was explicitly unregistered and the session was closed safely.
     */
    CLOSED(false);

    private final boolean active;

    State(boolean active) {
      this.active = active;
    }

    /**
     * Returns a boolean value indicating whether the state is an active state.
     * <p>
     * Sessions can only submit commands and receive events while in an active state.
     *
     * @return Indicates whether the state is an active state.
     */
    public boolean active() {
      return active;
    }

  }

  private final ClientSessionState state;
  private final ClientConnection connection;
  private final ClientSessionManager manager;
  private final ClientSessionListener listener;
  private final ClientSessionSubmitter submitter;

  public ClientSession(String id, ProtocolClient client, AddressSelector selector, ThreadContext context, ConnectionStrategy connectionStrategy, Duration sessionTimeout) {
    this(new ClientConnection(id, client, selector), new ClientSessionState(id), context, connectionStrategy, sessionTimeout);
  }

  private ClientSession(ClientConnection connection, ClientSessionState state, ThreadContext context, ConnectionStrategy connectionStrategy, Duration sessionTimeout) {
    this.connection = Assert.notNull(connection, "connection");
    this.state = Assert.notNull(state, "state");
    this.manager = new ClientSessionManager(connection, state, context, connectionStrategy, sessionTimeout);
    ClientSequencer sequencer = new ClientSequencer(state);
    this.listener = new ClientSessionListener(connection, state, sequencer, context);
    this.submitter = new ClientSessionSubmitter(connection, state, sequencer, context);
  }

  /**
   * Returns the session ID.
   * <p>
   * The session ID is unique to an individual session within the cluster. That is, it is guaranteed that
   * no two clients will have a session with the same ID.
   *
   * @return The session ID.
   */
  public long id() {
    return state.getSessionId();
  }

  /**
   * Returns the current session state.
   *
   * @return The current session state.
   */
  public State state() {
    return state.getState();
  }

  /**
   * Registers a callback to be called when the session state changes.
   *
   * @param callback The callback to be called when the session state changes.
   * @return The state change listener.
   */
  public Listener<State> onStateChange(Consumer<State> callback) {
    return state.onStateChange(callback);
  }

  /**
   * Submits a command to the session.
   *
   * @param command The command to submit.
   * @return A completable future to be completed with the command result.
   */
  public CompletableFuture<byte[]> submitCommand(byte[] command) {
    State state = state();
    if (state == State.CLOSED || state == State.EXPIRED) {
      return Futures.exceptionalFuture(new ClosedSessionException("session closed"));
    }
    return submitter.submitCommand(command);
  }

  /**
   * Submits a query to the session.
   *
   * @param query The query to submit.
   * @return A completable future to be completed with the query result.
   */
  public CompletableFuture<byte[]> submitQuery(byte[] query, ConsistencyLevel consistency) {
    State state = state();
    if (state == State.CLOSED || state == State.EXPIRED) {
      return Futures.exceptionalFuture(new ClosedSessionException("session closed"));
    }
    return submitter.submitQuery(query, consistency);
  }

  /**
   * Opens the session.
   *
   * @return A completable future to be completed once the session is opened.
   */
  public CompletableFuture<ClientSession> register() {
    return manager.open().thenApply(v -> this);
  }

  /**
   * Registers an event listener.
   * <p>
   * The registered {@link Consumer} will be {@link Consumer#accept(Object) called} when an event is received
   * from the Raft cluster for the session.
   *
   * @param callback The session receive callback.
   * @return The listener context.
   * @throws NullPointerException if {@code event} or {@code callback} is null
   */
  public Listener<byte[]> onEvent(Consumer<byte[]> callback) {
    return listener.onEvent(callback);
  }

  /**
   * Closes the session.
   *
   * @return A completable future to be completed once the session is closed.
   */
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    submitter.close()
      .thenCompose(v -> listener.close())
      .thenCompose(v -> manager.close())
      .whenComplete((managerResult, managerError) -> {
        connection.close().whenComplete((connectionResult, connectionError) -> {
          if (managerError != null) {
            future.completeExceptionally(managerError);
          } else if (connectionError != null) {
            future.completeExceptionally(connectionError);
          } else {
            future.complete(null);
          }
        });
      });
    return future;
  }

  /**
   * Expires the session.
   *
   * @return A completable future to be completed once the session has been expired.
   */
  public CompletableFuture<Void> expire() {
    return manager.expire();
  }

  /**
   * Kills the session.
   *
   * @return A completable future to be completed once the session has been killed.
   */
  public CompletableFuture<Void> kill() {
    return submitter.close()
      .thenCompose(v -> listener.close())
      .thenCompose(v -> manager.kill())
      .thenCompose(v -> connection.close());
  }

  @Override
  public int hashCode() {
    int hashCode = 31;
    long id = id();
    hashCode = 37 * hashCode + (int) (id ^ (id >>> 32));
    return hashCode;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof ClientSession && ((ClientSession) object).id() == id();
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d]", getClass().getSimpleName(), id());
  }

}
