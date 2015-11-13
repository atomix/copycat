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
package io.atomix.copycat.server.state;

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.concurrent.ComposableFuture;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.error.InternalException;
import io.atomix.copycat.client.error.UnknownSessionException;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.entry.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;

/**
 * Raft server state machine.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ServerStateMachine implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerStateMachine.class);
  private final StateMachine stateMachine;
  private final ClusterState cluster;
  private final ServerStateMachineExecutor executor;
  private final ServerCommitCleaner cleaner;
  private final ServerCommitPool commits;
  private long lastApplied;
  private long lastCompleted;
  private long configuration;

  ServerStateMachine(StateMachine stateMachine, ClusterState cluster, ServerStateMachineContext context, ServerCommitCleaner cleaner, ThreadContext executor) {
    this.stateMachine = stateMachine;
    this.cluster = cluster;
    this.executor = new ServerStateMachineExecutor(context, executor);
    this.cleaner = cleaner;
    this.commits = new ServerCommitPool(cleaner, this.executor.context().sessions());
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
   * <p>
   * The last applied index is updated *after* each time a non-query entry is applied to the state machine.
   *
   * @param lastApplied The last applied index.
   */
  private void setLastApplied(long lastApplied) {
    // If the last applied index decreased then that's very concerning.
    Assert.argNot(lastApplied < this.lastApplied, "lastApplied index must be greater than previous lastApplied index");
    if (lastApplied > this.lastApplied) {
      this.lastApplied = lastApplied;

      // Update the index for each session. This will be used to trigger queries that are awaiting the
      // application of specific indexes to the state machine. Setting the session index may cause query
      // callbacks to be called and queries to be evaluated.
      for (ServerSession session : executor.context().sessions().sessions.values()) {
        session.setVersion(lastApplied);
      }
    }
  }

  /**
   * Returns the highest index completed for all sessions.
   * <p>
   * The lastCompleted index is representative of the highest index for which related events have been
   * received by *all* clients. In other words, no events lower than the given index should remain in
   * memory.
   *
   * @return The highest index completed for all sessions.
   */
  long getLastCompleted() {
    return lastCompleted > 0 ? lastCompleted : lastApplied;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  CompletableFuture<?> apply(Entry entry) {
    return apply(entry, false);
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @param expectResult Indicates whether this call expects a result.
   * @return The result.
   */
  CompletableFuture<?> apply(Entry entry, boolean expectResult) {
    boolean apply = !(entry instanceof QueryEntry);
    try {
      if (!apply) {
        return apply((QueryEntry) entry);
      } else if (entry instanceof CommandEntry) {
        return apply((CommandEntry) entry, expectResult);
      } else if (entry instanceof RegisterEntry) {
        return apply((RegisterEntry) entry, expectResult);
      } else if (entry instanceof KeepAliveEntry) {
        return apply((KeepAliveEntry) entry);
      } else if (entry instanceof UnregisterEntry) {
        return apply((UnregisterEntry) entry, expectResult);
      } else if (entry instanceof HeartbeatEntry) {
        return apply((HeartbeatEntry) entry);
      } else if (entry instanceof NoOpEntry) {
        return apply((NoOpEntry) entry);
      } else if (entry instanceof ConnectEntry) {
        return apply((ConnectEntry) entry);
      } else if (entry instanceof ConfigurationEntry) {
        return apply((ConfigurationEntry) entry);
      }
      return Futures.exceptionalFuture(new InternalException("unknown state machine operation"));
    } finally {
      // After the entry has been applied, update the lastApplied index.
      if (apply) {
        setLastApplied(entry.getIndex());
      }
    }
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  private CompletableFuture<Void> apply(ConfigurationEntry entry) {
    long previousConfiguration = configuration;
    configuration = entry.getIndex();
    // Immediately clean the commit for the previous configuration since configuration entries
    // completely override the previous configuration.
    if (previousConfiguration > 0) {
      cleaner.clean(previousConfiguration);
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  private CompletableFuture<Void> apply(ConnectEntry entry) {
    // Connections are stored in the state machine when they're *written* to the log, so we need only
    // clean them once they're committed.
    cleaner.clean(entry.getIndex());
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  private CompletableFuture<Long> apply(RegisterEntry entry, boolean synchronous) {
    ServerSession session = new ServerSession(entry.getIndex(), executor.context(), entry.getTimeout());
    executor.context().sessions().registerSession(session);

    // Allow the executor to execute any scheduled events.
    long timestamp = executor.tick(entry.getTimestamp());

    // Update the session timestamp *after* executing any scheduled operations. The executor's timestamp
    // is guaranteed to be monotonically increasing, whereas the RegisterEntry may have an earlier timestamp
    // if, e.g., it was written shortly after a leader change.
    session.setTimestamp(timestamp);

    // Determine whether any sessions appear to be expired. This won't immediately expire the session(s),
    // but it will make them available to be unregistered by the leader.
    suspectSessions(timestamp);

    ThreadContext context = ThreadContext.currentContextOrThrow();
    long index = entry.getIndex();

    // Call the register() method on the user-provided state machine to allow the state machine to react to
    // a new session being registered. User state machine methods are always called in the state machine thread.
    CompletableFuture<Long> future = new ComposableFuture<>();
    executor.executor().execute(() -> {
      // Update the state machine context with the register entry's index. This ensures that events published
      // within the register method will be properly associated with the unregister entry's index. All events
      // published during registration of a session are linearizable to ensure that clients receive related events
      // before the registration is completed.
      executor.context().update(index, Instant.ofEpochMilli(timestamp), synchronous, Command.ConsistencyLevel.LINEARIZABLE);

      // Register the session and then open it. This ensures that state machines cannot publish events to this
      // session before the client has learned of the session ID.
      stateMachine.register(session);
      session.open();

      // Once register callbacks have been completed, ensure that events published during the callbacks are
      // received by clients. The state machine context will generate an event future for all published events
      // to all sessions.
      CompletableFuture<Void> sessionFuture = executor.context().commit();
      if (sessionFuture != null) {
        sessionFuture.whenComplete((result, error) -> {
          context.executor().execute(() -> future.complete(index));
        });
      } else {
        context.executor().execute(() -> future.complete(index));
      }
    });

    // Update the highest index completed for all sessions to allow log compaction to progress.
    updateLastCompleted(index);

    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   */
  private CompletableFuture<Void> apply(KeepAliveEntry entry) {
    ServerSession session = executor.context().sessions().getSession(entry.getSession());

    // Update the deterministic executor time and allow the executor to execute any scheduled events.
    long timestamp = executor.tick(entry.getTimestamp());

    // Determine whether any sessions appear to be expired. This won't immediately expire the session(s),
    // but it will make them available to be unregistered by the leader. Note that it's safe to trigger
    // scheduled executor callbacks even if the keep-alive entry is for an unknown session since the
    // leader still committed the entry with its time and so time will still progress deterministically.
    suspectSessions(timestamp);

    CompletableFuture<Void> future;

    // If the server session is null, the session either never existed or already expired.
    if (session == null) {
      LOGGER.warn("Unknown session: " + entry.getSession());
      future = Futures.exceptionalFuture(new UnknownSessionException("unknown session: " + entry.getSession()));
    }
    // If the session exists, don't allow it to expire even if its expiration has passed since we still
    // managed to receive a keep alive request from the client before it was removed. This allows the
    // client some arbitrary leeway in keeping its session alive. It's up to the leader to explicitly
    // expire a session by committing an UnregisterEntry in order to ensure sessions can't be expired
    // during leadership changes.
    else {
      ThreadContext context = ThreadContext.currentContextOrThrow();

      // Set the session as trusted. This will prevent the leader from explicitly unregistering the
      // session if it hasn't done so already.
      session.trust();

      // Update the session's timestamp with the current state machine time.
      session.setTimestamp(timestamp);

      // Store the command/event sequence and event version instead of acquiring a reference to the entry.
      long commandSequence = entry.getCommandSequence();
      long eventVersion = entry.getEventVersion();

      // The keep-alive entry also serves to clear cached command responses and events from memory.
      // Remove responses and clear/resend events in the state machine thread to prevent thread safety issues.
      executor.executor().execute(() -> session.clearResponses(commandSequence).resendEvents(eventVersion));

      // Since the index of acked events in the session changed, update the highest index completed for all
      // sessions to allow log compaction to progress. This is only done during session operations and not
      // within sessions themselves.
      updateLastCompleted(entry.getIndex());

      future = new CompletableFuture<>();
      context.executor().execute(() -> future.complete(null));
    }

    // Immediately clean the keep alive entry from the log.
    cleaner.clean(entry.getIndex());

    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  private CompletableFuture<Void> apply(UnregisterEntry entry, boolean synchronous) {
    ServerSession session = executor.context().sessions().unregisterSession(entry.getSession());

    // Update the deterministic executor time and allow the executor to execute any scheduled events.
    long timestamp = executor.tick(entry.getTimestamp());

    // Determine whether any sessions appear to be expired. This won't immediately expire the session(s),
    // but it will make them available to be unregistered by the leader. Note that it's safe to trigger
    // scheduled executor callbacks even if the keep-alive entry is for an unknown session since the
    // leader still committed the entry with its time and so time will still progress deterministically.
    suspectSessions(timestamp);

    CompletableFuture<Void> future;

    // If the server session is null, the session either never existed or already expired.
    if (session == null) {
      LOGGER.warn("Unknown session: " + entry.getSession());
      future = Futures.exceptionalFuture(new UnknownSessionException("unknown session: " + entry.getSession()));
    }
    // If the session exists, don't allow it to expire even if its expiration has passed since we still
    // managed to receive a keep alive request from the client before it was removed.
    else {
      ThreadContext context = ThreadContext.currentContextOrThrow();
      future = new CompletableFuture<>();

      long index = entry.getIndex();

      // If the entry was marked expired, that indicates that the leader explicitly expired the session due to
      // the session not being kept alive by the client. In all other cases, we close the session normally.
      if (entry.isExpired()) {
        executor.executor().execute(() -> {
          // Update the state machine context with the unregister entry's index. This ensures that events published
          // within the expire or close methods will be properly associated with the unregister entry's index.
          // All events published during expiration or closing of a session are linearizable to ensure that clients
          // receive related events before the expiration is completed.
          executor.context().update(index, Instant.ofEpochMilli(timestamp), synchronous, Command.ConsistencyLevel.LINEARIZABLE);

          // Expire the session and call state machine callbacks.
          session.expire();
          stateMachine.expire(session);
          stateMachine.close(session);

          // Once expiration callbacks have been completed, ensure that events published during the callbacks
          // are published in batch. The state machine context will generate an event future for all published events
          // to all sessions. If the event future is non-null, that indicates events are pending which were published
          // during the call to expire(). Wait for the events to be received by the client before completing the future.
          CompletableFuture<Void> sessionFuture = executor.context().commit();
          if (sessionFuture != null) {
            sessionFuture.whenComplete((result, error) -> {
              context.executor().execute(() -> future.complete(null));
            });
          } else {
            context.executor().execute(() -> future.complete(null));
          }
        });
      }
      // If the unregister entry is not indicated as expired, a client must have submitted a request to unregister
      // the session. In that case, we simply close the session without expiring it.
      else {
        executor.executor().execute(() -> {
          // Update the state machine context with the unregister entry's index. This ensures that events published
          // within the close method will be properly associated with the unregister entry's index. All events published
          // during expiration or closing of a session are linearizable to ensure that clients receive related events
          // before the expiration is completed.
          executor.context().update(index, Instant.ofEpochMilli(timestamp), synchronous, Command.ConsistencyLevel.LINEARIZABLE);

          // Close the session and call state machine callbacks.
          session.close();
          stateMachine.close(session);

          // Once close callbacks have been completed, ensure that events published during the callbacks
          // are published in batch. The state machine context will generate an event future for all published events
          // to all sessions. If the event future is non-null, that indicates events are pending which were published
          // during the call to expire(). Wait for the events to be received by the client before completing the future.
          CompletableFuture<Void> sessionFuture = executor.context().commit();
          if (sessionFuture != null) {
            sessionFuture.whenComplete((result, error) -> {
              context.executor().execute(() -> future.complete(null));
            });
          } else {
            context.executor().execute(() -> future.complete(null));
          }
        });
      }

      // Clean the unregister entry from the log immediately after it's applied.
      cleaner.clean(session.id());

      // Update the highest index completed for all sessions. This will be used to indicate the highest
      // index for which logs can be compacted.
      updateLastCompleted(entry.getIndex());
    }

    // Immediately clean the unregister entry from the log.
    cleaner.clean(entry.getIndex());

    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @param synchronous Whether the call expects a result.
   * @return The result.
   */
  private CompletableFuture<Object> apply(CommandEntry entry, boolean synchronous) {
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
    else if (entry.getSequence() > 0 && entry.getSequence() < session.nextSequence()) {
      // Ensure the response check is executed in the state machine thread in order to ensure the
      // command was applied, otherwise there will be a race condition and concurrent modification issues.
      ThreadContext context = ThreadContext.currentContextOrThrow();
      long sequence = entry.getSequence();

      // Get the consistency level of the command. This should match the consistency level of the original command.
      Command.ConsistencyLevel consistency = entry.getCommand().consistency();

      // Switch to the state machine thread and get the existing response.
      executor.executor().execute(() -> {

        // If the command's consistency level is not LINEARIZABLE or null (which are equivalent), return the
        // cached response immediately in the server thread.
        if (consistency == Command.ConsistencyLevel.NONE || consistency == Command.ConsistencyLevel.SEQUENTIAL) {
          Object response = session.getResponse(sequence);
          if (response == null) {
            context.executor().execute(() -> future.complete(null));
          } else if (response instanceof Throwable) {
            context.executor().execute(() -> future.completeExceptionally((Throwable) response));
          } else {
            context.executor().execute(() -> future.complete(response));
          }
        } else {
          // For linearizable commands, check whether a future is registered for the command. A future will be
          // registered if the original command resulted in publishing events to any session. For linearizable
          // commands, we wait until the event future is completed, indicating that all sessions to which event
          // messages were sent have received/acked the messages.
          CompletableFuture<Void> sessionFuture = session.getResponseFuture(sequence);
          if (sessionFuture != null) {
            sessionFuture.whenComplete((result, error) -> {
              Object response = session.getResponse(sequence);
              if (response == null) {
                context.executor().execute(() -> future.complete(null));
              } else if (response instanceof Throwable) {
                context.executor().execute(() -> future.completeExceptionally((Throwable) response));
              } else {
                context.executor().execute(() -> future.complete(response));
              }
            });
          } else {
            // If no event future was registered for the original command, return the cached response in the
            // server thread.
            Object response = session.getResponse(sequence);
            if (response == null) {
              context.executor().execute(() -> future.complete(null));
            } else if (response instanceof Throwable) {
              context.executor().execute(() -> future.completeExceptionally((Throwable) response));
            } else {
              context.executor().execute(() -> future.complete(response));
            }
          }
        }
      });
    }
    // If we've made it this far, the command must have been applied in the proper order as sequenced by the
    // session. This should be the case for most commands applied to the state machine.
    else {
      executeCommand(entry, session, synchronous, future, ThreadContext.currentContextOrThrow());
    }

    return future;
  }

  /**
   * Executes a state machine command.
   */
  private CompletableFuture<Object> executeCommand(CommandEntry entry, ServerSession session, boolean synchronous, CompletableFuture<Object> future, ThreadContext context) {
    context.checkThread();

    // Allow the executor to execute any scheduled events.
    long timestamp = executor.tick(entry.getTimestamp());
    long sequence = entry.getSequence();

    Command.ConsistencyLevel consistency = entry.getCommand().consistency();

    // Execute the command in the state machine thread. Once complete, the CompletableFuture callback will be completed
    // in the state machine thread. Register the result in that thread and then complete the future in the caller's thread.
    ServerCommit commit = commits.acquire(entry, timestamp);
    executor.executor().execute(() -> {

      // Update the state machine context with the commit index and local server context. The synchronous flag
      // indicates whether the server expects linearizable completion of published events. Events will be published
      // based on the configured consistency level for the context.
      executor.context().update(commit.index(), commit.time(), synchronous, consistency != null ? consistency : Command.ConsistencyLevel.LINEARIZABLE);

      try {
        // Execute the state machine operation and get the result.
        Object result = executor.executeOperation(commit);

        // Once the operation has been applied to the state machine, commit events published by the command.
        // The state machine context will build a composite future for events published to all sessions.
        CompletableFuture<Void> sessionFuture = executor.context().commit();

        // If the command consistency level is not LINEARIZABLE or null, register the response and complete the
        // command immediately in the state machine thread. Note that we don't store the event future since
        // the sequential nature of the command means we shouldn't need to block even on retries.
        if (consistency == Command.ConsistencyLevel.NONE || consistency == Command.ConsistencyLevel.SEQUENTIAL) {
          session.registerResponse(sequence, result, null);
          context.executor().execute(() -> future.complete(result));
        } else {
          // If the command consistency level is LINEARIZABLE, store the response with the event future. The stored
          // response will be used to provide linearizable semantics for commands resubmitted to the cluster.
          // If an event future was provided by the state machine context indicating that events were published by
          // the command, wait for the events to be received and acknowledged by the respective sessions before returning.
          session.registerResponse(sequence, result, sessionFuture);
          if (sessionFuture != null) {
            sessionFuture.whenComplete((sessionResult, sessionError) -> {
              context.executor().execute(() -> future.complete(result));
            });
          } else {
            context.executor().execute(() -> future.complete(result));
          }
        }
      } catch (Exception e) {
        // If an exception occurs during execution of the command, store the exception.
        session.registerResponse(sequence, e, null);
        context.executor().execute(() -> future.completeExceptionally(e));
      }
    });

    // Update the session timestamp and command sequence number. This is done in the caller's thread since all
    // timestamp/version/sequence checks are done in this thread prior to executing operations on the state machine thread.
    session.setTimestamp(timestamp).setSequence(sequence);

    return future;
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
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
      ThreadContext context = ThreadContext.currentContextOrThrow();

      // Store the entry version and sequence instead of acquiring a reference to the entry.
      long version = entry.getVersion();
      long sequence = entry.getSequence();

      // Once the query has met its sequence requirement, check whether it has also met its version requirement. If the version
      // requirement is not yet met, queue the query for the state machine to catch up to the required version.
      ServerCommit commit = commits.acquire(entry, executor.timestamp());
      session.registerSequenceQuery(sequence, () -> {
        context.checkThread();
        if (version > session.getVersion()) {
          session.registerVersionQuery(version, () -> {
            context.checkThread();
            executeQuery(commit, future, context);
          });
        } else {
          executeQuery(commit, future, context);
        }
      });
      return future;
    }
    // If the query version number is greater than the current version number for the session, queue the query.
    else if (entry.getVersion() > session.getVersion()) {
      CompletableFuture<Object> future = new CompletableFuture<>();

      ThreadContext context = ThreadContext.currentContextOrThrow();

      ServerCommit commit = commits.acquire(entry, executor.timestamp());
      session.registerVersionQuery(entry.getVersion(), () -> {
        context.checkThread();
        executeQuery(commit, future, context);
      });
      return future;
    } else {
      return executeQuery(commits.acquire(entry, executor.timestamp()), new CompletableFuture<>(), ThreadContext.currentContextOrThrow());
    }
  }

  /**
   * Executes a state machine query.
   */
  private CompletableFuture<Object> executeQuery(ServerCommit commit, CompletableFuture<Object> future, ThreadContext context) {
    executor.executor().execute(() -> {
      executor.context().update(commit.index(), commit.time(), true, null);
      try {
        Object result = executor.executeOperation(commit);
        context.executor().execute(() -> future.complete(result));
      } catch (Exception e) {
        context.executor().execute(() -> future.completeExceptionally(e));
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
  private CompletableFuture<Void> apply(HeartbeatEntry entry) {
    long timestamp = executor.tick(entry.getTimestamp());

    // Set the member status to AVAILABLE and update the member heartbeat time.
    MemberState member = cluster.getMember(entry.getMember());
    if (member != null) {
      member.setHeartbeatTime(timestamp).setStatus(MemberState.Status.AVAILABLE);
    }

    // Iterate through all members and update statuses based on the heartbeat time.
    for (MemberState memberState : cluster.getMembers()) {
      if (timestamp - memberState.getHeartbeatTime() > memberState.getHeartbeatTimeout()) {
        memberState.setStatus(MemberState.Status.UNAVAILABLE);
      }
    }
    return Futures.completedFutureAsync(null, ThreadContext.currentContextOrThrow().executor());
  }

  /**
   * Applies an entry to the state machine.
   *
   * @param entry The entry to apply.
   * @return The result.
   */
  private CompletableFuture<Long> apply(NoOpEntry entry) {
    // Iterate through all the server sessions and reset timestamps. This ensures that sessions do not
    // timeout during leadership changes or shortly thereafter.
    long timestamp = executor.tick(entry.getTimestamp());
    for (ServerSession session : executor.context().sessions().sessions.values()) {
      session.setTimestamp(timestamp);
    }
    cleaner.clean(entry.getIndex());
    return Futures.completedFutureAsync(entry.getIndex(), ThreadContext.currentContextOrThrow().executor());
  }

  /**
   * Updates the last completed event version based on a commit at the given index.
   */
  private void updateLastCompleted(long index) {
    long lastCompleted = index;
    for (ServerSession session : executor.context().sessions().sessions.values()) {
      lastCompleted = Math.min(lastCompleted, session.getLastCompleted());
    }

    if (lastCompleted < this.lastCompleted)
      throw new IllegalStateException("inconsistent session state");
    this.lastCompleted = lastCompleted;
  }

  /**
   * Suspects any sessions that have timed out.
   */
  private void suspectSessions(long timestamp) {
    for (ServerSession session : executor.context().sessions().sessions.values()) {
      if (timestamp - session.timeout() > session.getTimestamp()) {
        session.suspect();
      }
    }
  }

  @Override
  public void close() {
    executor.close();
  }

}
