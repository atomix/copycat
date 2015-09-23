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
 * limitations under the License.
 */
package io.atomix.catalog.server.state;

import io.atomix.catalog.client.Command;
import io.atomix.catalog.client.error.InternalException;
import io.atomix.catalog.client.error.UnknownSessionException;
import io.atomix.catalog.server.StateMachine;
import io.atomix.catalog.server.storage.entry.*;
import io.atomix.catalyst.util.concurrent.ComposableFuture;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Raft server state machine.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ServerStateMachine implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerStateMachine.class);
  private final StateMachine stateMachine;
  private final ServerStateMachineExecutor executor;
  private final ServerCommitPool commits;
  private long lastApplied;

  ServerStateMachine(StateMachine stateMachine, ServerCommitCleaner cleaner, ThreadContext context) {
    this.stateMachine = stateMachine;
    this.executor = new ServerStateMachineExecutor(context);
    this.commits = new ServerCommitPool(cleaner, executor.context().sessions());
    init();
  }

  /**
   * Initializes the state machine.
   */
  private void init() {
    stateMachine.init(executor);
  }

  /**
   * Returns the server state machine executor.
   *
   * @return The server state machine executor.
   */
  ServerStateMachineExecutor executor() {
    return executor;
  }

  /**
   * Returns the last applied index.
   *
   * @return The last applied index.
   */
  long getLastApplied() {
    return lastApplied;
  }

  /**
   * Sets the last applied index.
   *
   * @param lastApplied The last applied index.
   */
  private void setLastApplied(long lastApplied) {
    if (lastApplied < this.lastApplied) {
      throw new IllegalArgumentException("lastApplied index must be greater than previous lastApplied index");
    } else if (lastApplied > this.lastApplied) {
      this.lastApplied = lastApplied;

      for (ServerSession session : executor.context().sessions().sessions.values()) {
        session.setVersion(lastApplied);
      }
    }
  }

  /**
   * Returns the current thread context.
   *
   * @return The current thread context.
   */
  private ThreadContext getContext() {
    ThreadContext context = ThreadContext.currentContext();
    if (context == null)
      throw new IllegalStateException("must be called from a Catalyst thread");
    return context;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  CompletableFuture<?> apply(Entry entry) {
    try {
      if (entry instanceof CommandEntry) {
        return apply((CommandEntry) entry);
      } else if (entry instanceof QueryEntry) {
        return apply((QueryEntry) entry);
      } else if (entry instanceof RegisterEntry) {
        return apply((RegisterEntry) entry);
      } else if (entry instanceof KeepAliveEntry) {
        return apply((KeepAliveEntry) entry);
      } else if (entry instanceof NoOpEntry) {
        return apply((NoOpEntry) entry);
      }
      return Futures.exceptionalFuture(new InternalException("unknown state machine operation"));
    } finally {
      setLastApplied(entry.getIndex());
    }
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  private CompletableFuture<Long> apply(RegisterEntry entry) {
    ServerSession session = executor.context().sessions().registerSession(entry.getIndex(), entry.getConnection(), entry.getTimeout()).setTimestamp(entry.getTimestamp());

    // Allow the executor to execute any scheduled events.
    executor.tick(entry.getTimestamp());

    // Expire any remaining expired sessions.
    expireSessions(entry.getTimestamp());

    ThreadContext context = getContext();
    long index = entry.getIndex();

    // Set last applied only after the operation has been submitted to the state machine executor.
    CompletableFuture<Long> future = new ComposableFuture<>();
    executor.executor().execute(() -> {
      stateMachine.register(session);
      context.execute(() -> future.complete(index));
    });

    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   */
  private CompletableFuture<Void> apply(KeepAliveEntry entry) {
    ServerSession session = executor.context().sessions().getSession(entry.getSession());

    // Allow the executor to execute any scheduled events.
    executor.tick(entry.getTimestamp());

    // Expire any remaining expired sessions.
    expireSessions(entry.getTimestamp());

    CompletableFuture<Void> future;

    // If the server session is null, the session either never existed or already expired.
    if (session == null) {
      LOGGER.warn("Unknown session: " + entry.getSession());
      future = Futures.exceptionalFuture(new UnknownSessionException("unknown session: " + entry.getSession()));
    }
    // If the session exists, don't allow it to expire even if its expiration has passed since we still
    // managed to receive a keep alive request from the client before it was removed.
    else {
      ThreadContext context = getContext();

      // The keep alive request contains the
      session.setTimestamp(entry.getTimestamp())
        .clearResponses(entry.getCommandSequence())
        .clearEvents(entry.getEventVersion(), entry.getEventSequence());

      future = new CompletableFuture<>();
      context.execute(() -> future.complete(null));
    }

    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Object> apply(CommandEntry entry) {
    final CompletableFuture<Object> future = new CompletableFuture<>();

    // First check to ensure that the session exists.
    ServerSession session = executor.context().sessions().getSession(entry.getSession());

    // If the session is null then that indicates that the session already timed out or it never existed.
    // Return with an UnknownSessionException.
    if (session == null) {
      LOGGER.warn("Unknown session: " + entry.getSession());
      future.completeExceptionally(new UnknownSessionException("unknown session: " + entry.getSession()));
    }
    // If the command's sequence number is less than the next session sequence number then that indicates that
    // we've received a command that was previously applied to the state machine. Ensure linearizability by
    // returning the cached response instead of applying it to the user defined state machine.
    else if (entry.getSequence() < session.nextSequence()) {
      // Ensure the response check is executed in the state machine thread in order to ensure the
      // command was applied, otherwise there will be a race condition and concurrent modification issues.
      ThreadContext context = getContext();
      long sequence = entry.getSequence();

      executor.executor().execute(() -> {
        Object response = session.getResponse(sequence);
        if (response == null) {
          context.executor().execute(() -> future.complete(null));
        } else if (response instanceof Throwable) {
          context.executor().execute(() -> future.completeExceptionally((Throwable) response));
        } else {
          context.executor().execute(() -> future.complete(response));
        }
      });
    }
    // If we've made it this far, the command must have been applied in the proper order as sequenced by the
    // session. This should be the case for most commands applied to the state machine.
    else {
      executeCommand(entry, session, future, getContext());
    }

    return future;
  }

  /**
   * Executes a state machine command.
   */
  private CompletableFuture<Object> executeCommand(CommandEntry entry, ServerSession session, CompletableFuture<Object> future, ThreadContext context) {
    context.checkThread();

    // Allow the executor to execute any scheduled events.
    executor.tick(entry.getTimestamp());

    long sequence = entry.getSequence();

    // Execute the command in the state machine thread. Once complete, the CompletableFuture callback will be completed
    // in the state machine thread. Register the result in that thread and then complete the future in the caller's thread.
    executor.execute(commits.acquire(entry)).whenComplete((result, error) -> {
      if (error == null) {
        session.registerResponse(sequence, result);
        context.execute(() -> future.complete(result));
      } else {
        session.registerResponse(sequence, error);
        context.execute(() -> future.completeExceptionally((Throwable) error));
      }
    });

    // Update the session timestamp and command sequence number. This is done in the caller's thread since all
    // timestamp/version/sequence checks are done in this thread prior to executing operations on the state machine thread.
    session.setTimestamp(entry.getTimestamp());

    // If the command's consistency is LINEARIZABLE, update the session command sequence number.
    if (entry.getCommand().consistency() == Command.ConsistencyLevel.LINEARIZABLE) {
      session.setSequence(entry.getSequence());
    }

    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<Object> apply(QueryEntry entry) {
    ServerSession session = executor.context().sessions().getSession(entry.getSession());

    // If the session is null then that indicates that the session already timed out or it never existed.
    // Return with an UnknownSessionException.
    if (session == null) {
      LOGGER.warn("Unknown session: " + entry.getSession());
      return Futures.exceptionalFuture(new UnknownSessionException("unknown session " + entry.getSession()));
    }
    // Query execution is determined by the sequence and version supplied for the query. All queries are queued until the state
    // machine advances at least until the provided sequence and version.
    // If the query sequence number is greater than the current sequence number for the session, queue the query.
    else if (entry.getSequence() > session.getSequence()) {
      CompletableFuture<Object> future = new CompletableFuture<>();

      // Get the caller's context.
      ThreadContext context = getContext();

      // Once the query has met its sequence requirement, check whether it has also met its version requirement. If the version
      // requirement is not yet met, queue the query for the state machine to catch up to the required version.
      session.registerSequenceQuery(entry.getSequence(), () -> {
        context.checkThread();
        if (entry.getVersion() > session.getVersion()) {
          session.registerVersionQuery(entry.getVersion(), () -> {
            context.checkThread();
            executeQuery(entry, future, context);
          });
        } else {
          executeQuery(entry, future, context);
        }
      });
      return future;
    }
    // If the query version number is greater than the current version number for the session, queue the query.
    else if (entry.getVersion() > session.getVersion()) {
      CompletableFuture<Object> future = new CompletableFuture<>();

      ThreadContext context = getContext();

      session.registerVersionQuery(entry.getVersion(), () -> {
        context.checkThread();
        executeQuery(entry, future, context);
      });
      return future;
    } else {
      return executeQuery(entry, new CompletableFuture<>(), getContext());
    }
  }

  /**
   * Executes a state machine query.
   */
  private CompletableFuture<Object> executeQuery(QueryEntry entry, CompletableFuture<Object> future, ThreadContext context) {
    executor.execute(commits.acquire(entry.setTimestamp(executor.timestamp()))).whenComplete((result, error) -> {
      if (error == null) {
        context.execute(() -> future.complete(result));
      } else {
        context.execute(() -> future.completeExceptionally((Throwable) error));
      }
    });
    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  private CompletableFuture<Long> apply(NoOpEntry entry) {
    // Iterate through all the server sessions and reset timestamps. This ensures that sessions do not
    // timeout during leadership changes.
    for (ServerSession session : executor.context().sessions().sessions.values()) {
      session.setTimestamp(entry.getTimestamp());
    }
    return Futures.completedFutureAsync(entry.getIndex(), getContext().executor());
  }

  /**
   * Expires any sessions that have timed out.
   */
  private void expireSessions(long timestamp) {
    Set<Long> sessions = new HashSet<>(executor.context().sessions().sessions.keySet());
    for (long sessionId : sessions) {
      ServerSession session = executor.context().sessions().getSession(sessionId);
      if (timestamp - session.timeout() > session.getTimestamp()) {
        executor.context().sessions().unregisterSession(sessionId);
        session.expire();
        stateMachine.expire(session);
      }
    }
  }

  @Override
  public void close() {
    executor.close();
  }

}
