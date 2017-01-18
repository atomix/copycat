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

import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Command;
import io.atomix.copycat.NoOpCommand;
import io.atomix.copycat.Query;
import io.atomix.copycat.error.CommandException;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.error.QueryException;
import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.atomix.copycat.protocol.response.CommandResponse;
import io.atomix.copycat.protocol.response.OperationResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.response.QueryResponse;
import io.atomix.copycat.session.ClosedSessionException;
import io.atomix.copycat.session.Session;

import java.net.ConnectException;
import java.net.ProtocolException;
import java.nio.channels.ClosedChannelException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

/**
 * Session operation submitter.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class ClientSessionSubmitter {
  private static final int[] FIBONACCI = new int[]{1, 1, 2, 3, 5};
  private static final Predicate<Throwable> EXCEPTION_PREDICATE = e -> e instanceof ConnectException || e instanceof TimeoutException || e instanceof ProtocolException || e instanceof ClosedChannelException;
  private final ProtocolClientConnection connection;
  private final ClientSessionState state;
  private final ClientSequencer sequencer;
  private final ThreadContext context;
  private final Map<Long, OperationAttempt> attempts = new LinkedHashMap<>();

  public ClientSessionSubmitter(ProtocolClientConnection connection, ClientSessionState state, ClientSequencer sequencer, ThreadContext context) {
    this.connection = Assert.notNull(connection, "connection");
    this.state = Assert.notNull(state, "state");
    this.sequencer = Assert.notNull(sequencer, "sequencer");
    this.context = Assert.notNull(context, "context");
  }

  /**
   * Submits a command to the cluster.
   *
   * @param command The command to submit.
   * @param <T> The command result type.
   * @return A completable future to be completed once the command has been submitted.
   */
  public <T> CompletableFuture<T> submit(Command<T> command) {
    CompletableFuture<T> future = new CompletableFuture<>();
    context.executor().execute(() -> submitCommand(command, future));
    return future;
  }

  /**
   * Submits a command to the cluster.
   */
  private <T> void submitCommand(Command<T> command, CompletableFuture<T> future) {
    submit(new CommandAttempt<>(sequencer.nextRequest(), state.getSessionId(), state.nextCommandRequest(), command, future));
  }

  /**
   * Submits a query to the cluster.
   *
   * @param query The query to submit.
   * @param <T> The query result type.
   * @return A completable future to be completed once the query has been submitted.
   */
  public <T> CompletableFuture<T> submit(Query<T> query) {
    CompletableFuture<T> future = new CompletableFuture<>();
    context.executor().execute(() -> submitQuery(query, future));
    return future;
  }

  /**
   * Submits a query to the cluster.
   */
  private <T> void submitQuery(Query<T> query, CompletableFuture<T> future) {
    submit(new QueryAttempt<>(sequencer.nextRequest(), state.getSessionId(), state.getCommandRequest(), state.getResponseIndex(), query, future));
  }

  /**
   * Submits an operation attempt.
   *
   * @param attempt The attempt to submit.
   */
  private <T extends OperationRequest, U extends OperationResponse, V> void submit(OperationAttempt<T, U, V> attempt) {
    if (state.getState() == Session.State.CLOSED || state.getState() == Session.State.EXPIRED) {
      attempt.fail(new ClosedSessionException("session closed"));
    } else {
      attempt.execute(connection).whenComplete(attempt);
      attempt.future.whenComplete((r, e) -> attempts.remove(attempt.id));
    }
  }

  /**
   * Closes the submitter.
   *
   * @return A completable future to be completed with a list of pending operations.
   */
  public CompletableFuture<Void> close() {
    for (OperationAttempt attempt : new ArrayList<>(attempts.values())) {
      attempt.fail(new ClosedSessionException("session closed"));
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Operation attempt.
   */
  private abstract class OperationAttempt<T extends OperationRequest, U extends OperationResponse, V> implements BiConsumer<U, Throwable> {
    protected final long id;
    protected final int attempt;
    protected final CompletableFuture<V> future;

    protected OperationAttempt(long id, int attempt, CompletableFuture<V> future) {
      this.id = id;
      this.attempt = attempt;
      this.future = future;
    }

    /**
     * Returns the next instance of the attempt.
     *
     * @return The next instance of the attempt.
     */
    protected abstract OperationAttempt<T, U, V> next();

    /**
     * Returns a new instance of the default exception for the operation.
     *
     * @return A default exception for the operation.
     */
    protected abstract Throwable defaultException();

    /**
     * Executes the attempt.
     *
     * @param connection The client connection.
     * @return The operation response future.
     */
    protected abstract CompletableFuture<U> execute(ProtocolClientConnection connection);

    /**
     * Completes the operation successfully.
     *
     * @param response The operation response.
     */
    protected abstract void complete(U response);

    /**
     * Completes the operation with an exception.
     *
     * @param error The completion exception.
     */
    protected void complete(Throwable error) {
      sequence(null, () -> future.completeExceptionally(error));
    }

    /**
     * Runs the given callback in proper sequence.
     *
     * @param response The operation response.
     * @param callback The callback to run in sequence.
     */
    protected final void sequence(OperationResponse response, Runnable callback) {
      sequencer.sequenceResponse(id, response, callback);
    }

    /**
     * Fails the attempt.
     */
    public void fail() {
      fail(defaultException());
    }

    /**
     * Fails the attempt with the given exception.
     *
     * @param t The exception with which to fail the attempt.
     */
    public void fail(Throwable t) {
      complete(t);
    }

    /**
     * Immediately retries the attempt.
     */
    public void retry() {
      context.executor().execute(() -> submit(next()));
    }

    /**
     * Retries the attempt after the given duration.
     *
     * @param after The duration after which to retry the attempt.
     */
    public void retry(Duration after) {
      context.schedule(after, () -> submit(next()));
    }
  }

  /**
   * Command operation attempt.
   */
  private final class CommandAttempt<T> extends OperationAttempt<CommandRequest, CommandResponse, T> {
    private final long session;
    private final long sequence;
    private final Command<T> command;

    public CommandAttempt(long id, long session, long sequence, Command<T> command, CompletableFuture<T> future) {
      super(id, 1, future);
      this.session = session;
      this.sequence = sequence;
      this.command = command;
    }

    public CommandAttempt(long id, int attempt, long session, long sequence, Command<T> command, CompletableFuture<T> future) {
      super(id, attempt, future);
      this.session = session;
      this.sequence = sequence;
      this.command = command;
    }

    @Override
    protected CompletableFuture<CommandResponse> execute(ProtocolClientConnection connection) {
      return connection.command(builder ->
        builder.withSession(session)
          .withSequence(sequence)
          .withCommand(command)
          .build());
    }

    @Override
    protected OperationAttempt<CommandRequest, CommandResponse, T> next() {
      return new CommandAttempt<>(id, this.attempt + 1, session, sequence, command, future);
    }

    @Override
    protected Throwable defaultException() {
      return new CommandException("failed to complete command");
    }

    @Override
    public void accept(CommandResponse response, Throwable error) {
      if (error == null) {
        state.getLogger().debug("{} - Received {}", state.getSessionId(), response);
        if (response.status() == ProtocolResponse.Status.OK) {
          complete(response);
        } else if (response.error() == CopycatError.Type.APPLICATION_ERROR) {
          complete(response.error().createException());
        } else if (response.error() != CopycatError.Type.UNKNOWN_SESSION_ERROR) {
          retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt-1, FIBONACCI.length-1)]));
        }
      } else if (EXCEPTION_PREDICATE.test(error) || (error instanceof CompletionException && EXCEPTION_PREDICATE.test(error.getCause()))) {
        retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt-1, FIBONACCI.length-1)]));
      } else {
        fail(error);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void fail(Throwable t) {
      super.fail(t);
      context.executor().execute(() -> submit(new CommandAttempt<>(id, this.attempt + 1, session, sequence, (Command) new NoOpCommand(), (CompletableFuture) future)));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void complete(CommandResponse response) {
      sequence(response, () -> {
        state.setCommandResponse(sequence);
        state.setResponseIndex(response.index());
        future.complete((T) response.result());
      });
    }
  }

  /**
   * Query operation attempt.
   */
  private final class QueryAttempt<T> extends OperationAttempt<QueryRequest, QueryResponse, T> {
    private final long session;
    private final long sequence;
    private final long index;
    private final Query<T> query;

    public QueryAttempt(long id, long sessionId, long sequence, long index, Query<T> query, CompletableFuture<T> future) {
      super(id, 1, future);
      this.session = sessionId;
      this.sequence = sequence;
      this.index = index;
      this.query = query;
    }

    public QueryAttempt(long id, int attempt, long sessionId, long sequence, long index, Query<T> query, CompletableFuture<T> future) {
      super(id, attempt, future);
      this.session = sessionId;
      this.sequence = sequence;
      this.index = index;
      this.query = query;
    }

    @Override
    protected CompletableFuture<QueryResponse> execute(ProtocolClientConnection connection) {
      return connection.query(builder ->
        builder.withSession(session)
          .withSequence(sequence)
          .withIndex(index)
          .withQuery(query)
          .build());
    }

    @Override
    protected OperationAttempt<QueryRequest, QueryResponse, T> next() {
      return new QueryAttempt<>(id, this.attempt + 1, session, sequence, index, query, future);
    }

    @Override
    protected Throwable defaultException() {
      return new QueryException("failed to complete query");
    }

    @Override
    public void accept(QueryResponse response, Throwable error) {
      if (error == null) {
        state.getLogger().debug("{} - Received {}", state.getSessionId(), response);
        if (response.status() == ProtocolResponse.Status.OK) {
          complete(response);
        } else {
          complete(response.error().createException());
        }
      } else {
        fail(error);
      }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void complete(QueryResponse response) {
      sequence(response, () -> {
        state.setResponseIndex(response.index());
        future.complete((T) response.result());
      });
    }
  }

}
