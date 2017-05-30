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

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Command;
import io.atomix.copycat.Operation;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.session.CopycatSession;
import io.atomix.copycat.session.Session;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Handles submitting state machine {@link Command commands} and {@link Query queries} to the Copycat cluster.
 * <p>
 * The client session is responsible for maintaining a client's connection to a Copycat cluster and coordinating
 * the submission of {@link Command commands} and {@link Query queries} to various nodes in the cluster. Client
 * sessions are single-use objects that represent the context within which a cluster can guarantee linearizable
 * semantics for state machine operations. When a session is opened, the session will register
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
public class DefaultCopycatSession implements CopycatSession {
  private final CopycatSessionState state;
  private final CopycatSessionManager sessionManager;
  private final CopycatSessionListener sessionListener;
  private final CopycatSessionSubmitter sessionSubmitter;
  private final ThreadContext context;

  public DefaultCopycatSession(CopycatSessionState state, CopycatConnection leaderConnection, CopycatConnection sessionConnection, ThreadContext threadContext, CopycatSessionManager sessionManager) {
    this.state = Assert.notNull(state, "state");
    this.sessionManager = Assert.notNull(sessionManager, "sessionManager");
    CopycatSessionSequencer sequencer = new CopycatSessionSequencer(state);
    this.sessionListener = new CopycatSessionListener(sessionConnection, state, sequencer, threadContext);
    this.sessionSubmitter = new CopycatSessionSubmitter(leaderConnection, sessionConnection, state, sequencer, sessionManager, threadContext);
    this.context = threadContext;
  }

  @Override
  public String name() {
    return state.getSessionName();
  }

  @Override
  public String type() {
    return state.getSessionType();
  }

  @Override
  public State state() {
    return state.getState();
  }

  @Override
  public Listener<State> onStateChange(Consumer<State> callback) {
    return state.onStateChange(s -> context.execute(() -> callback.accept(s)));
  }

  @Override
  public ThreadContext context() {
    return context;
  }

  /**
   * Submits an operation to the session.
   *
   * @param operation The operation to submit.
   * @param <T> The operation result type.
   * @return A completable future to be completed with the operation result.
   */
  public <T> CompletableFuture<T> submit(Operation<T> operation) {
    if (operation instanceof Query) {
      return submit((Query<T>) operation);
    } else if (operation instanceof Command) {
      return submit((Command<T>) operation);
    } else {
      throw new UnsupportedOperationException("unknown operation type: " + operation.getClass());
    }
  }

  /**
   * Submits a command to the session.
   *
   * @param command The command to submit.
   * @param <T> The command result type.
   * @return A completable future to be completed with the command result.
   */
  public <T> CompletableFuture<T> submit(Command<T> command) {
    return sessionSubmitter.submit(command);
  }

  /**
   * Submits a query to the session.
   *
   * @param query The query to submit.
   * @param <T> The query result type.
   * @return A completable future to be completed with the query result.
   */
  public <T> CompletableFuture<T> submit(Query<T> query) {
    return sessionSubmitter.submit(query);
  }

  /**
   * Registers a void event listener.
   * <p>
   * The registered {@link Runnable} will be {@link Runnable#run() called} when an event is received
   * from the Raft cluster for the session. {@link Session} implementations must guarantee that consumers are
   * always called in the same thread for the session. Therefore, no two events will be received concurrently
   * by the session. Additionally, events are guaranteed to be received in the order in which they were sent by
   * the state machine.
   *
   * @param event The event to which to listen.
   * @param callback The session receive callback.
   * @return The listener context.
   * @throws NullPointerException if {@code event} or {@code callback} is null
   */
  public Listener<Void> onEvent(String event, Runnable callback) {
    return sessionListener.onEvent(event, callback);
  }

  /**
   * Registers an event listener.
   * <p>
   * The registered {@link Consumer} will be {@link Consumer#accept(Object) called} when an event is received
   * from the Raft cluster for the session. {@link Session} implementations must guarantee that consumers are
   * always called in the same thread for the session. Therefore, no two events will be received concurrently
   * by the session. Additionally, events are guaranteed to be received in the order in which they were sent by
   * the state machine.
   *
   * @param event The event to which to listen.
   * @param callback The session receive callback.
   * @param <T> The session event type.
   * @return The listener context.
   * @throws NullPointerException if {@code event} or {@code callback} is null
   */
  public <T> Listener<T> onEvent(String event, Consumer<T> callback) {
    return sessionListener.onEvent(event, callback);
  }

  @Override
  public boolean isOpen() {
    return state.isOpen();
  }

  @Override
  public CompletableFuture<Void> close() {
    return sessionManager.closeSession(state.getSessionId()).whenComplete((result, error) -> state.close());
  }

  @Override
  public int hashCode() {
    int hashCode = 31;
    long id = state.getSessionId();
    hashCode = 37 * hashCode + (int) (id ^ (id >>> 32));
    return hashCode;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof DefaultCopycatSession && ((DefaultCopycatSession) object).state.getSessionId() == state.getSessionId();
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d]", getClass().getSimpleName(), state.getSessionId());
  }

}
