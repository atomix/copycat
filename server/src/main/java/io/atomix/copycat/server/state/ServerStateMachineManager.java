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

import io.atomix.catalyst.concurrent.ComposableFuture;
import io.atomix.catalyst.concurrent.Futures;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.concurrent.ThreadPoolContext;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.error.InternalException;
import io.atomix.copycat.error.UnknownClientException;
import io.atomix.copycat.error.UnknownSessionException;
import io.atomix.copycat.error.UnknownStateMachineException;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.LogCleaner;
import io.atomix.copycat.server.storage.entry.*;
import io.atomix.copycat.server.storage.snapshot.Snapshot;
import io.atomix.copycat.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * Internal server state machine.
 * <p>
 * The internal state machine handles application of commands to the user provided {@link StateMachine}
 * and keeps track of internal state like sessions and the various indexes relevant to log compaction.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ServerStateMachineManager implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerStateMachineManager.class);
  private final ServerContext state;
  private final ScheduledExecutorService threadPool;
  private final ThreadContext threadContext;
  private final Log log;
  private final ClientManager clientManager = new ClientManager();
  private final ServerSessionManager sessionManager = new ServerSessionManager();
  private final Map<String, ServerStateMachineExecutor> stateMachines = new HashMap<>();
  private long timestamp;
  private volatile long lastApplied;
  private long lastCompleted;
  private volatile Snapshot pendingSnapshot;

  public ServerStateMachineManager(ServerContext state, ScheduledExecutorService threadPool, ThreadContext threadContext) {
    this.state = Assert.notNull(state, "state");
    this.log = state.getLog();
    this.threadPool = threadPool;
    this.threadContext = threadContext;
  }

  /**
   * Returns the client manager.
   *
   * @return The client manager.
   */
  public ClientManager getClients() {
    return clientManager;
  }

  /**
   * Returns the session manager.
   *
   * @return The session manager.
   */
  public ServerSessionManager getSessions() {
    return sessionManager;
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
    // lastApplied should be either equal to or one greater than this.lastApplied.
    if (lastApplied > this.lastApplied) {
      Assert.arg(lastApplied == this.lastApplied + 1, "lastApplied must be sequential");
      this.lastApplied = lastApplied;
    } else {
      Assert.arg(lastApplied == this.lastApplied, "lastApplied cannot be decreased");
    }
  }

  /**
   * Updates and returns the current logical timestamp.
   *
   * @param entry The entry with which to update the timestamp.
   * @return The updated timestamp.
   */
  private long updateTimestamp(TimestampedEntry<?> entry) {
    timestamp = Math.max(this.timestamp, entry.getTimestamp());
    for (ServerStateMachineExecutor executor : stateMachines.values()) {
      executor.tick(entry.getIndex(), timestamp);
    }
    return timestamp;
  }

  /**
   * Updates the last completed event index based on a commit at the given index.
   */
  private void updateLastCompleted(long index) {
    if (!log.isOpen())
      return;

    // Calculate the last completed index as the lowest index acknowledged by all clients.
    long lastCompleted = index;
    for (ServerSessionContext session : sessionManager.getSessions()) {
      lastCompleted = Math.min(lastCompleted, session.getLastCompleted());
    }

    this.lastCompleted = Math.max(this.lastCompleted, lastCompleted);

    // Update the log compaction minor index.
    log.compactor().minorIndex(this.lastCompleted);
  }

  /**
   * Applies all commits up to the given index.
   * <p>
   * Calls to this method are assumed not to expect a result. This allows some optimizations to be
   * made internally since linearizable events don't have to be waited to complete the command.
   *
   * @param index The index up to which to apply commits.
   */
  public void applyAll(long index) {
    if (!log.isOpen())
      return;

    // If the effective commit index is greater than the last index applied to the state machine then apply remaining entries.
    long lastIndex = Math.min(index, log.lastIndex());
    if (lastIndex > lastApplied) {
      for (long i = lastApplied + 1; i <= lastIndex; i++) {
        Entry entry = log.get(i);
        if (entry != null) {
          apply(entry).whenComplete((result, error) -> entry.release());
        }
        setLastApplied(i);
      }
    }
  }

  /**
   * Applies the entry at the given index to the state machine.
   * <p>
   * Calls to this method are assumed to expect a result. This means linearizable session events
   * triggered by the application of the command at the given index will be awaited before completing
   * the returned future.
   *
   * @param index The index to apply.
   * @return A completable future to be completed once the commit has been applied.
   */
  public <T> CompletableFuture<T> apply(long index) {
    try {
      // If entries remain to be applied prior to this entry then synchronously apply them.
      if (index > lastApplied + 1) {
        applyAll(index - 1);
      }

      // Read the entry from the log. If the entry is non-null them apply the entry, otherwise
      // simply update the last applied index and return a null result.
      try (Entry entry = log.get(index)) {
        if (entry != null) {
          return apply(entry);
        } else {
          return CompletableFuture.completedFuture(null);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
      return Futures.exceptionalFuture(e);
    } finally {
      setLastApplied(index);
    }
  }

  /**
   * Applies an entry to the state machine.
   * <p>
   * Calls to this method are assumed to expect a result. This means linearizable session events
   * triggered by the application of the given entry will be awaited before completing the returned future.
   *
   * @param entry The entry to apply.
   * @return A completable future to be completed with the result.
   */
  @SuppressWarnings("unchecked")
  public <T> CompletableFuture<T> apply(Entry entry) {
    LOGGER.trace("{} - Applying {}", state.getCluster().member().address(), entry);
    ThreadContext context = ThreadContext.currentContextOrThrow();
    ComposableFuture<T> future = new ComposableFuture<T>();
    BiConsumer<T, Throwable> callback = (result, error) -> {
      if (error == null) {
        context.execute(() -> future.complete(result));
      } else {
        context.execute(() -> future.completeExceptionally(error));
      }
    };

    threadContext.execute(() -> {
      if (entry instanceof QueryEntry) {
        ((CompletableFuture<T>) apply((QueryEntry) entry)).whenComplete(callback);
      } else if (entry instanceof CommandEntry) {
        ((CompletableFuture<T>) apply((CommandEntry) entry)).whenComplete(callback);
      } else if (entry instanceof OpenSessionEntry) {
        ((CompletableFuture<T>) apply((OpenSessionEntry) entry)).whenComplete(callback);
      } else if (entry instanceof CloseSessionEntry) {
        ((CompletableFuture<T>) apply((CloseSessionEntry) entry)).whenComplete(callback);
      } else if (entry instanceof RegisterEntry) {
        ((CompletableFuture<T>) apply((RegisterEntry) entry)).whenComplete(callback);
      } else if (entry instanceof KeepAliveEntry) {
        ((CompletableFuture<T>) apply((KeepAliveEntry) entry)).whenComplete(callback);
      } else if (entry instanceof UnregisterEntry) {
        ((CompletableFuture<T>) apply((UnregisterEntry) entry)).whenComplete(callback);
      } else if (entry instanceof InitializeEntry) {
        ((CompletableFuture<T>) apply((InitializeEntry) entry)).whenComplete(callback);
      } else if (entry instanceof ConfigurationEntry) {
        ((CompletableFuture<T>) apply((ConfigurationEntry) entry)).whenComplete(callback);
      } else {
        future.completeExceptionally(new InternalException("Unknown entry type"));
      }
    });
    return future;
  }

  /**
   * Applies a configuration entry to the internal state machine.
   * <p>
   * Configuration entries are applied to internal server state when written to the log. Thus, no significant
   * logic needs to take place in the handling of configuration entries. We simply release the previous configuration
   * entry since it was overwritten by a more recent committed configuration entry.
   */
  private CompletableFuture<Void> apply(ConfigurationEntry entry) {
    // Clean the configuration entry from the log. The entry will be retained until it has been stored
    // on all servers.
    log.release(entry.getIndex());
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Applies register session entry to the state machine.
   * <p>
   * Register entries are applied to the state machine to create a new session. The resulting session ID is the
   * index of the RegisterEntry. Once a new session is registered, we call register() on the state machine.
   * In the event that the {@code synchronous} flag is set, that indicates that the registration command expects a
   * response, i.e. it was applied by a leader. In that case, any events published during the execution of the
   * state machine's register() method must be completed synchronously prior to the completion of the returned future.
   */
  private CompletableFuture<Long> apply(RegisterEntry entry) {
    long index = entry.getIndex();
    long timestamp = updateTimestamp(entry);

    ClientContext client = new ClientContext(entry.getIndex(), entry.getTimeout(), new LogCleaner(log));
    clientManager.registerClient(client);

    // Update the session timestamp *after* executing any scheduled operations. The executor's timestamp
    // is guaranteed to be monotonically increasing, whereas the RegisterEntry may have an earlier timestamp
    // if, e.g., it was written shortly after a leader change.
    client.open(timestamp);

    // Determine whether any sessions appear to be expired. This won't immediately expire the session(s),
    // but it will make them available to be unregistered by the leader.
    suspectClients(client.id(), timestamp);

    // Update the last completed index to allow event entries to be compacted.
    updateLastCompleted(index);

    return CompletableFuture.completedFuture(client.id());
  }

  /**
   * Applies a session keep alive entry to the state machine.
   * <p>
   * Keep alive entries are applied to the internal state machine to reset the timeout for a specific session.
   * If the session indicated by the KeepAliveEntry is still held in memory, we mark the session as trusted,
   * indicating that the client has committed a keep alive within the required timeout. Additionally, we check
   * all other sessions for expiration based on the timestamp provided by this KeepAliveEntry. Note that sessions
   * are never completely expired via this method. Leaders must explicitly commit an UnregisterEntry to expire
   * a session.
   * <p>
   * When a KeepAliveEntry is committed to the internal state machine, two specific fields provided in the entry
   * are used to update server-side session state. The {@code commandSequence} indicates the highest command for
   * which the session has received a successful response in the proper sequence. By applying the {@code commandSequence}
   * to the server session, we clear command output held in memory up to that point. The {@code eventVersion} indicates
   * the index up to which the client has received event messages in sequence for the session. Applying the
   * {@code eventVersion} to the server-side session results in events up to that index being removed from memory
   * as they were acknowledged by the client. It's essential that both of these fields be applied via entries committed
   * to the Raft log to ensure they're applied on all servers in sequential order.
   * <p>
   * Keep alive entries are retained in the log until the next time the client sends a keep alive entry or until the
   * client's session is expired. This ensures for sessions that have long timeouts, keep alive entries cannot be cleaned
   * from the log before they're replicated to some servers.
   */
  private CompletableFuture<Void> apply(KeepAliveEntry entry) {
    ClientContext client = clientManager.getClient(entry.getClient());

    // Update the deterministic executor time and allow the executor to execute any scheduled events.
    long timestamp = updateTimestamp(entry);

    // Determine whether any sessions appear to be expired. This won't immediately expire the session(s),
    // but it will make them available to be unregistered by the leader. Note that it's safe to trigger
    // scheduled executor callbacks even if the keep-alive entry is for an unknown session since the
    // leader still committed the entry with its time and so time will still progress deterministically.
    suspectClients(entry.getClient(), timestamp);

    // If the server session is null, the session either never existed or already expired.
    if (client == null) {
      log.release(entry.getIndex());
      return Futures.exceptionalFuture(new UnknownSessionException("Unknown client: " + entry.getClient()));
    }
    // If the session is in an inactive state, return an UnknownSessionException.
    else if (client.state() == ClientContext.State.CLOSED) {
      log.release(entry.getIndex());
      return Futures.exceptionalFuture(new UnknownSessionException("Inactive client: " + entry.getClient()));
    }

    // If the session exists, don't allow it to expire even if its expiration has passed since we still
    // managed to receive a keep alive request from the client before it was removed. This allows the
    // client some arbitrary leeway in keeping its session alive. It's up to the leader to explicitly
    // expire a session by committing an UnregisterEntry in order to ensure sessions can't be expired
    // during leadership changes.
    long index = entry.getIndex();

    // Set the session as trusted. This will prevent the leader from explicitly unregistering the
    // session if it hasn't done so already.
    client.keepAlive(index, timestamp);

    // Store the session/command/event sequence and event index instead of acquiring a reference to the entry.
    long[] sessionIds = entry.getSessionIds();
    long[] commandSequences = entry.getCommandSequences();
    long[] eventIndexes = entry.getEventIndexes();
    long[] connections = entry.getConnections();

    for (int i = 0; i < sessionIds.length; i++) {
      long sessionId = sessionIds[i];
      long commandSequence = commandSequences[i];
      long eventIndex = eventIndexes[i];
      long connection = connections[i];

      // Register the connection with the session manager. This will cause the session manager to remove connections
      // from sessions if the session isn't currently connected to this server.
      sessionManager.registerConnection(sessionId, connection);

      ServerSessionContext session = sessionManager.getSession(sessionId);
      if (session != null) {
        session.getStateMachineExecutor().keepAlive(index, timestamp, session, commandSequence, eventIndex);
      }
    }

    // Update the last completed index to allow event entries to be compacted.
    updateLastCompleted(index);

    return CompletableFuture.completedFuture(null);
  }

  /**
   * Applies an unregister session entry to the state machine.
   * <p>
   * Unregister entries may either be committed by clients or by the cluster's leader. Clients will commit
   * an unregister entry when closing their session normally. Leaders will commit an unregister entry when
   * an expired session is detected. This ensures that sessions are never expired purely on gaps in the log
   * which may result from normal log cleaning or lengthy leadership changes.
   * <p>
   * If the session was unregistered by the client, the isExpired flag will be false. Sessions expired by
   * the client are only close()ed on the state machine but not expire()d. Alternatively, entries where
   * isExpired is true were committed by a leader. For expired sessions, the state machine's expire() method
   * is called before close().
   * <p>
   * State machines may publish events during the handling of session expired or closed events. If the
   * {@code synchronous} flag passed to this method is true, events published during the commitment of the
   * UnregisterEntry must be synchronously completed prior to the completion of the returned future. This
   * ensures that state changes resulting from the expiration or closing of a session are completed before
   * the session close itself is completed.
   */
  private CompletableFuture<Void> apply(UnregisterEntry entry) {
    // Get the session from the context sessions. Note that we do not unregister the session here. Sessions
    // can only be unregistered once all references to session commands have been released by the state machine.
    ClientContext client = clientManager.unregisterClient(entry.getClient());

    // Update the deterministic executor time and allow the executor to execute any scheduled events.
    long timestamp = updateTimestamp(entry);

    // Determine whether any sessions appear to be expired. This won't immediately expire the session(s),
    // but it will make them available to be unregistered by the leader. Note that it's safe to trigger
    // scheduled executor callbacks even if the keep-alive entry is for an unknown session since the
    // leader still committed the entry with its time and so time will still progress deterministically.
    suspectClients(entry.getClient(), timestamp);

    // If the server session is null, the session either never existed or already expired.
    if (client == null) {
      log.release(entry.getIndex());
      return Futures.exceptionalFuture(new UnknownSessionException("Unknown client: " + entry.getClient()));
    }
    // If the session is not in an active state, return an UnknownSessionException.
    else if (client.state() == ClientContext.State.CLOSED) {
      log.release(entry.getIndex());
      return Futures.exceptionalFuture(new UnknownSessionException("Inactive client: " + entry.getClient()));
    }

    long index = entry.getIndex();

    // If the entry was marked expired, that indicates that the leader explicitly expired the session due to
    // the session not being kept alive by the client. In all other cases, we close the session normally.
    if (entry.isExpired()) {
      sessionManager.getSessions().stream()
        .filter(s -> s.client() == client.id())
        .forEach(s -> s.getStateMachineExecutor().expire(index, timestamp, s));
    }
    // If the unregister entry is not indicated as expired, a client must have submitted a request to unregister
    // the session. In that case, we simply close the session without expiring it.
    else {
      sessionManager.getSessions().stream()
        .filter(s -> s.client() == client.id())
        .forEach(s -> s.getStateMachineExecutor().unregister(index, timestamp, s));
    }

    // Close the client.
    client.close(index);

    // Update the last completed index to allow event entries to be compacted.
    updateLastCompleted(index);

    return CompletableFuture.completedFuture(null);
  }

  /**
   * Applies an open session entry to the state machine.
   */
  private CompletableFuture<Long> apply(OpenSessionEntry entry) {
    ClientContext client = clientManager.getClient(entry.getClient());

    long index = entry.getIndex();
    long timestamp = updateTimestamp(entry);

    // If the server session is null, the session either never existed or already expired.
    if (client == null) {
      log.release(entry.getIndex());
      return Futures.exceptionalFuture(new UnknownClientException("Unknown client: " + entry.getClient()));
    }
    // If the session is not in an active state, return an UnknownSessionException.
    else if (client.state() == ClientContext.State.CLOSED) {
      log.release(entry.getIndex());
      return Futures.exceptionalFuture(new UnknownClientException("Inactive client: " + entry.getClient()));
    }

    // Get the state machine executor or create one if it doesn't already exist.
    ServerStateMachineExecutor stateMachineExecutor = stateMachines.get(entry.getName());
    if (stateMachineExecutor == null) {
      Supplier<StateMachine> stateMachineSupplier = state.getStateMachineRegistry().getFactory(entry.getType());
      if (stateMachineSupplier == null) {
        return Futures.exceptionalFuture(new UnknownStateMachineException("Unknown state machine type " + entry.getType()));
      }
      stateMachineExecutor = new ServerStateMachineExecutor(stateMachineSupplier.get(), state, new ThreadPoolContext(threadPool, threadContext.serializer().clone()));
      stateMachines.put(entry.getName(), stateMachineExecutor);
    }

    // Create and register the session.
    ServerSessionContext session = new ServerSessionContext(entry.getIndex(), entry.getClient(), log, stateMachineExecutor);
    sessionManager.registerSession(session);

    // Return the session ID for commands.
    return session.getStateMachineExecutor().register(index, timestamp, session).thenApplyAsync(v -> session.id(), threadContext);
  }

  /**
   * Applies a close session entry to the state machine.
   */
  private CompletableFuture<Void> apply(CloseSessionEntry entry) {
    ServerSessionContext session = sessionManager.getSession(entry.getSession());

    long index = entry.getIndex();
    long timestamp = updateTimestamp(entry);

    // If the server session is null, the session either never existed or already expired.
    if (session == null) {
      log.release(entry.getIndex());
      return Futures.exceptionalFuture(new UnknownSessionException("Unknown session: " + entry.getSession()));
    }
    // If the session is not in an active state, return an UnknownSessionException.
    else if (session.state() == Session.State.CLOSED) {
      log.release(entry.getIndex());
      return Futures.exceptionalFuture(new UnknownSessionException("Inactive session: " + entry.getSession()));
    }

    // Get the state machine executor associated with the session and unregister the session.
    ServerStateMachineExecutor stateMachineExecutor = session.getStateMachineExecutor();
    return stateMachineExecutor.unregister(index, timestamp, session).thenApplyAsync(v -> v, threadContext);
  }

  /**
   * Applies a command entry to the state machine.
   * <p>
   * Command entries result in commands being executed on the user provided {@link StateMachine} and a
   * response being sent back to the client by completing the returned future. All command responses are
   * cached in the command's {@link ServerSessionContext} for fault tolerance. In the event that the same command
   * is applied to the state machine more than once, the original response will be returned.
   * <p>
   * Command entries are written with a sequence number. The sequence number is used to ensure that
   * commands are applied to the state machine in sequential order. If a command entry has a sequence
   * number that is less than the next sequence number for the session, that indicates that it is a
   * duplicate of a command that was already applied. Otherwise, commands are assumed to have been
   * received in sequential order. The reason for this assumption is because leaders always sequence
   * commands as they're written to the log, so no sequence number will be skipped.
   */
  private CompletableFuture<OperationResult> apply(CommandEntry entry) {
    long index = entry.getIndex();
    long timestamp = updateTimestamp(entry);
    long sequenceNumber = entry.getSequence();
    Command command = entry.getCommand();

    // First check to ensure that the session exists.
    ServerSessionContext session = sessionManager.getSession(entry.getSession());

    // If the session is null, return an UnknownSessionException. Commands applied to the state machine must
    // have a session. We ensure that session register/unregister entries are not compacted from the log
    // until all associated commands have been cleaned.
    if (session == null) {
      log.release(entry.getIndex());
      return Futures.exceptionalFuture(new UnknownSessionException("unknown session: " + entry.getSession()));
    }
    // If the session is not in an active state, return an UnknownSessionException. Sessions are retained in the
    // session registry until all prior commands have been released by the state machine, but new commands can
    // only be applied for sessions in an active state.
    else if (!session.state().active()) {
      log.release(entry.getIndex());
      return Futures.exceptionalFuture(new UnknownSessionException("inactive session: " + entry.getSession()));
    }

    // Execute the command using the state machine associated with the session.
    return session.getStateMachineExecutor().executeCommand(index, timestamp, sequenceNumber, session, command);
  }

  /**
   * Applies a query entry to the state machine.
   * <p>
   * Query entries are applied to the user {@link StateMachine} for read-only operations.
   * Because queries are read-only, they may only be applied on a single server in the cluster,
   * and query entries do not go through the Raft log. Thus, it is critical that measures be taken
   * to ensure clients see a consistent view of the cluster event when switching servers. To do so,
   * clients provide a sequence and version number for each query. The sequence number is the order
   * in which the query was sent by the client. Sequence numbers are shared across both commands and
   * queries. The version number indicates the last index for which the client saw a command or query
   * response. In the event that the lastApplied index of this state machine does not meet the provided
   * version number, we wait for the state machine to catch up before applying the query. This ensures
   * clients see state progress monotonically even when switching servers.
   * <p>
   * Because queries may only be applied on a single server in the cluster they cannot result in the
   * publishing of session events. Events require commands to be written to the Raft log to ensure
   * fault-tolerance and consistency across the cluster.
   */
  private CompletableFuture<OperationResult> apply(QueryEntry entry) {
    ServerSessionContext session = sessionManager.getSession(entry.getSession());

    long index = entry.getIndex();
    long sequence = entry.getSequence();
    long timestamp = updateTimestamp(entry);
    Query query = entry.getQuery();

    // If the session is null then that indicates that the session already timed out or it never existed.
    // Return with an UnknownSessionException.
    if (session == null) {
      return Futures.exceptionalFuture(new UnknownSessionException("unknown session " + entry.getSession()));
    }
    // If the session is not in an active state, return an UnknownSessionException. Sessions are retained in the
    // session registry until all prior commands have been released by the state machine, but new operations can
    // only be applied for sessions in an active state.
    else if (!session.state().active()) {
      return Futures.exceptionalFuture(new UnknownSessionException("inactive session: " + entry.getSession()));
    }

    // Execute the query using the state machine associated with the session.
    return session.getStateMachineExecutor().executeQuery(index, sequence, timestamp, session, query);
  }

  /**
   * Applies an initialize entry to the state machine.
   * <p>
   * Initialize entries are committed by leaders at the start of their term. Typically, no-op entries
   * serve as a mechanism to allow leaders to commit entries from prior terms. However, we extend
   * the functionality of the no-op entry to use it as an indicator that a leadership change occurred.
   * In order to ensure timeouts do not expire during lengthy leadership changes, we use no-op entries
   * to reset timeouts for client sessions and server heartbeats.
   */
  private CompletableFuture<Long> apply(InitializeEntry entry) {
    // Iterate through all the server sessions and reset timestamps. This ensures that sessions do not
    // timeout during leadership changes or shortly thereafter.
    long index = entry.getIndex();
    long timestamp = updateTimestamp(entry);
    for (ClientContext clientContext : clientManager.getClients()) {
      clientContext.keepAlive(index, timestamp);
    }
    log.release(entry.getIndex());
    return Futures.completedFutureAsync(entry.getIndex(), ThreadContext.currentContextOrThrow());
  }

  /**
   * Marked as suspicious any sessions that have timed out according to the given timestamp.
   * <p>
   * Sessions are marked suspicious instead of being expired since log cleaning can result in large
   * gaps in time between entries in the log. Thus, once log compaction has occurred, it's possible
   * that a session could be marked expired when in fact its keep alive entries were simply compacted
   * from the log. Forcing the leader to expire sessions ensures that keep alives are not missed with
   * regard to session expiration.
   */
  private void suspectClients(long exclude, long timestamp) {
    for (ClientContext clientContext : clientManager.getClients()) {
      if (clientContext.id() != exclude && timestamp - clientContext.timeout() > clientContext.getTimestamp()) {
        clientContext.suspect(timestamp);
      }
    }
  }

  @Override
  public void close() {
    threadContext.close();
  }
}
