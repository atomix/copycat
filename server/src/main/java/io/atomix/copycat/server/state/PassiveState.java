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

import io.atomix.copycat.ConsistencyLevel;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.protocol.error.ProtocolException;
import io.atomix.copycat.protocol.request.ConnectRequest;
import io.atomix.copycat.protocol.request.QueryRequest;
import io.atomix.copycat.protocol.response.ConnectResponse;
import io.atomix.copycat.protocol.response.OperationResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.response.QueryResponse;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.request.AcceptRequest;
import io.atomix.copycat.server.protocol.request.AppendRequest;
import io.atomix.copycat.server.protocol.request.InstallRequest;
import io.atomix.copycat.server.protocol.response.AppendResponse;
import io.atomix.copycat.server.protocol.response.InstallResponse;
import io.atomix.copycat.server.storage.Indexed;
import io.atomix.copycat.server.storage.LogReader;
import io.atomix.copycat.server.storage.LogWriter;
import io.atomix.copycat.server.storage.entry.ConnectEntry;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.server.storage.entry.QueryEntry;
import io.atomix.copycat.server.storage.snapshot.Snapshot;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * Passive state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class PassiveState extends ReserveState {
  private Snapshot pendingSnapshot;
  private int nextSnapshotOffset;

  public PassiveState(ServerContext context) {
    super(context);
  }

  @Override
  public CopycatServer.State type() {
    return CopycatServer.State.PASSIVE;
  }

  @Override
  public CompletableFuture<ServerState> open() {
    return super.open()
      .thenRun(this::truncateUncommittedEntries)
      .thenApply(v -> this);
  }

  /**
   * Truncates uncommitted entries from the log.
   */
  private void truncateUncommittedEntries() {
    if (type() == CopycatServer.State.PASSIVE) {
      final LogWriter writer = context.getLogWriter();
      try {
        writer.lock();
        writer.truncate(context.getCommitIndex());
      } finally {
        writer.unlock();
      }
    }
  }

  @Override
  public CompletableFuture<ConnectResponse> onConnect(ConnectRequest request, ProtocolServerConnection connection) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(
        ConnectResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      // Immediately register the session connection and send an accept request to the leader.
      context.getStateMachine().context().sessions().registerConnection(request.client(), connection);

      return forward(c -> c.accept(AcceptRequest.builder()
        .withClient(request.client())
        .withAddress(context.getCluster().member().serverAddress())
        .build()))
        .thenApply(acceptResponse ->
          ConnectResponse.builder()
            .withStatus(ProtocolResponse.Status.OK)
            .withLeader(context.getLeader() != null ? context.getLeader().clientAddress() : null)
            .withMembers(context.getCluster().members().stream()
              .map(Member::clientAddress)
              .filter(m -> m != null)
              .collect(Collectors.toList()))
            .build())
        .exceptionally(error ->
          ConnectResponse.builder()
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(final AppendRequest request) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());

    return CompletableFuture.completedFuture(logResponse(handleAppend(request)));
  }

  /**
   * Handles an append request.
   */
  protected AppendResponse handleAppend(AppendRequest request) {
    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.getTerm()) {
      LOGGER.debug("{} - Rejected {}: request term is less than the current term ({})", context.getCluster().member().address(), request, context.getTerm());
      return AppendResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLogWriter().lastIndex())
        .build();
    } else {
      return checkGlobalIndex(request);
    }
  }

  /**
   * Checks whether the log needs to be truncated based on the globalIndex.
   */
  protected AppendResponse checkGlobalIndex(AppendRequest request) {
    // If the globalIndex has changed and is not present in the local log, truncate the log.
    // This ensures that if major compaction progressed on any server beyond the entries in this
    // server's log that this server receives all entries after compaction.
    // If the current global index is 0 then do not perform the index check. This ensures that
    // servers don't truncate their logs at startup.
    // Ensure that the globalIndex is updated here to prevent endlessly truncating the log
    // if the AppendRequest is rejected.
    long currentGlobalIndex = context.getGlobalIndex();
    long nextGlobalIndex = request.globalIndex();
    if (currentGlobalIndex > 0 && nextGlobalIndex > currentGlobalIndex && nextGlobalIndex > context.getLogWriter().lastIndex()) {
      context.setGlobalIndex(nextGlobalIndex);
      context.reset();
    }

    // If an entry was provided, check the entry against the local log.
    if (request.logIndex() != 0) {
      return checkPreviousEntry(request);
    } else {
      return appendEntries(request);
    }
  }

  /**
   * Checks the previous entry in the append request for consistency.
   */
  protected AppendResponse checkPreviousEntry(AppendRequest request) {
    final long lastIndex = context.getLogWriter().lastIndex();
    if (request.logIndex() != 0 && request.logIndex() > lastIndex) {
      LOGGER.debug("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getCluster().member().address(), request, request.logIndex(), lastIndex);
      return AppendResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLogWriter().lastIndex())
        .build();
    }

    // If the previous entry term doesn't match the local previous term then reject the request.
    final Indexed<? extends Entry<?>> previousEntry = context.getLogReader().get(request.logIndex());
    if (previousEntry == null || previousEntry.term() != request.logTerm()) {
      LOGGER.debug("{} - Rejected {}: Request log term does not match local log term {} for the same entry", context.getCluster().member().address(), request, (previousEntry != null ? previousEntry.term() : null));
      return AppendResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(request.logIndex() <= lastIndex ? request.logIndex() - 1 : lastIndex)
        .build();
    } else {
      return appendEntries(request);
    }
  }

  /**
   * Appends entries to the local log.
   */
  @SuppressWarnings("unchecked")
  protected AppendResponse appendEntries(AppendRequest request) {
    // Get the last entry index or default to the request log index.
    long lastEntryIndex = request.logIndex();
    if (!request.entries().isEmpty()) {
      lastEntryIndex = request.entries().get(request.entries().size() - 1).index();
    }

    // Ensure the commitIndex is not increased beyond the index of the last entry in the request.
    final long commitIndex = Math.max(context.getCommitIndex(), Math.min(request.commitIndex(), lastEntryIndex));

    // Get the server log reader/writer.
    final LogReader reader = context.getLogReader();
    final LogWriter writer = context.getLogWriter();

    // If the request entries are non-empty, write them to the log.
    if (!request.entries().isEmpty()) {
      writer.lock();
      try {
        for (Indexed<? extends Entry> entry : request.entries()) {
          // If the entry is a connect entry then immediately configure the connection.
          if (entry.entry().type() == Entry.Type.CONNECT) {
            Indexed<ConnectEntry> connectEntry = (Indexed<ConnectEntry>) entry;
            context.getStateMachine().context().sessions().registerAddress(connectEntry.entry().client(), connectEntry.entry().address());
          }

          // If the entry index is greater than the commitIndex, break the loop.
          if (entry.index() > commitIndex) {
            break;
          }

          // Read the existing entry from the log. If the entry does not exist in the log,
          // append it. If the entry's term is different than the term of the entry in the log,
          // overwrite the entry in the log. This will force the log to be truncated if necessary.
          Indexed<? extends Entry> existing = reader.get(entry.index());
          if (existing == null || existing.term() != entry.term()) {
            writer.append(entry);
            LOGGER.debug("{} - Appended {}", context.getCluster().member().address(), entry);
          }
        }
      } finally {
        writer.unlock();
      }
    }

    // Update the context commit and global indices.
    context.setCommitIndex(commitIndex);
    context.setGlobalIndex(request.globalIndex());

    // Apply commits to the state machine in batch.
    context.getStateMachine().applyAll(context.getCommitIndex());

    return AppendResponse.builder()
      .withStatus(ProtocolResponse.Status.OK)
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .withLogIndex(writer.lastIndex())
      .build();
  }

  @Override
  public CompletableFuture<QueryResponse> onQuery(QueryRequest request) {
    context.checkThread();
    logRequest(request);

    // If the query was submitted with RYW or monotonic read consistency, attempt to apply the query to the local state machine.
    if (request.consistency() == ConsistencyLevel.SEQUENTIAL) {

      // If this server has not yet applied entries up to the client's session ID, forward the
      // query to the leader. This ensures that a follower does not tell the client its session
      // doesn't exist if the follower hasn't had a chance to see the session's registration entry.
      if (context.getStateMachine().getLastApplied() < request.session()) {
        LOGGER.debug("{} - State out of sync, forwarding query to leader");
        return queryForward(request);
      }

      // If the commit index is not in the log then we've fallen too far behind the leader to perform a local query.
      // Forward the request to the leader.
      if (context.getLogWriter().lastIndex() < context.getCommitIndex()) {
        LOGGER.debug("{} - State out of sync, forwarding query to leader");
        return queryForward(request);
      }

      final Indexed<QueryEntry> entry = new Indexed<>(
        request.index(),
        context.getTerm(),
        new QueryEntry(
          System.currentTimeMillis(),
          request.session(),
          request.sequence(),
          request.bytes()), 0);

      return queryLocal(entry);
    } else {
      return queryForward(request);
    }
  }

  /**
   * Forwards the query to the leader.
   */
  private CompletableFuture<QueryResponse> queryForward(QueryRequest request) {
    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(
        QueryResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
          .build()));
    }

    LOGGER.debug("{} - Forwarded {}", context.getCluster().member().address(), request);
    return forward(connection -> connection.query(QueryRequest.builder()
      .withSession(request.session())
      .withSequence(request.sequence())
      .withIndex(request.index())
      .withQuery(request.bytes())
      .withConsistency(request.consistency())
      .build()))
      .thenApply(response ->
        QueryResponse.builder()
          .withStatus(response.status())
          .withError(response.error())
          .withIndex(response.index())
          .withEventIndex(response.eventIndex())
          .withResult(response.result())
          .build())
      .exceptionally(error ->
        QueryResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
          .build())
      .thenApply(this::logResponse);
  }

  /**
   * Performs a local query.
   */
  protected CompletableFuture<QueryResponse> queryLocal(Indexed<QueryEntry> entry) {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    sequenceQuery(entry, future);
    return future;
  }

  /**
   * Sequences the given query.
   */
  private void sequenceQuery(Indexed<QueryEntry> entry, CompletableFuture<QueryResponse> future) {
    // Get the client's server session. If the session doesn't exist, return an unknown session error.
    ServerSession session = context.getStateMachine().context().sessions().getSession(entry.entry().session());
    if (session == null) {
      future.complete(logResponse(
        QueryResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.UNKNOWN_SESSION_ERROR)
          .build()));
    } else {
      sequenceQuery(entry, session, future);
    }
  }

  /**
   * Sequences the given query.
   */
  private void sequenceQuery(Indexed<QueryEntry> entry, ServerSession session, CompletableFuture<QueryResponse> future) {
    // If the query's sequence number is greater than the session's current sequence number, queue the request for
    // handling once the state machine is caught up.
    if (entry.entry().sequence() > session.getCommandSequence()) {
      session.registerSequenceQuery(entry.entry().sequence(), () -> indexQuery(entry, future));
    } else {
      indexQuery(entry, future);
    }
  }

  /**
   * Ensures the given query is applied after the appropriate index.
   */
  private void indexQuery(Indexed<QueryEntry> entry, CompletableFuture<QueryResponse> future) {
    // Get the client's server session. If the session doesn't exist, return an unknown session error.
    ServerSession session = context.getStateMachine().context().sessions().getSession(entry.entry().session());
    if (session == null) {
      future.complete(logResponse(
        QueryResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.UNKNOWN_SESSION_ERROR)
          .build()));
    } else {
      indexQuery(entry, session, future);
    }
  }

  /**
   * Ensures the given query is applied after the appropriate index.
   */
  private void indexQuery(Indexed<QueryEntry> entry, ServerSession session, CompletableFuture<QueryResponse> future) {
    // If the query index is greater than the session's last applied index, queue the request for handling once the
    // state machine is caught up.
    if (entry.index() > session.getLastApplied()) {
      session.registerIndexQuery(entry.index(), () -> applyQuery(entry, future));
    } else {
      applyQuery(entry, future);
    }
  }

  /**
   * Applies a query to the state machine.
   */
  protected CompletableFuture<QueryResponse> applyQuery(Indexed<QueryEntry> entry, CompletableFuture<QueryResponse> future) {
    // In the case of the leader, the state machine is always up to date, so no queries will be queued and all query
    // indexes will be the last applied index.
    context.getStateMachine().<ServerStateMachine.Result>apply(entry).whenComplete((result, error) -> {
      completeOperation(result, QueryResponse.builder(), error, future);
    });
    return future;
  }

  /**
   * Completes an operation.
   */
  protected <T extends OperationResponse> void completeOperation(ServerStateMachine.Result result, OperationResponse.Builder<?, T> builder, Throwable error, CompletableFuture<T> future) {
    if (isOpen()) {
      if (result != null) {
        builder.withIndex(result.index);
        builder.withEventIndex(result.eventIndex);
        if (result.error != null) {
          error = result.error;
        }
      }

      if (error == null) {
        future.complete(logResponse(builder.withStatus(ProtocolResponse.Status.OK)
          .withResult(result.result.readBytes())
          .build()));
      } else if (error instanceof CompletionException && error.getCause() instanceof ProtocolException) {
        future.complete(logResponse(builder.withStatus(ProtocolResponse.Status.ERROR)
          .withError(((ProtocolException) error.getCause()).getType())
          .build()));
      } else if (error instanceof ProtocolException) {
        future.complete(logResponse(builder.withStatus(ProtocolResponse.Status.ERROR)
          .withError(((ProtocolException) error).getType())
          .build()));
      } else {
        future.complete(logResponse(builder.withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.INTERNAL_ERROR)
          .build()));
      }
    }
  }

  @Override
  public CompletableFuture<InstallResponse> onInstall(InstallRequest request) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());

    // If the request is for a lesser term, reject the request.
    if (request.term() < context.getTerm()) {
      return CompletableFuture.completedFuture(logResponse(
        InstallResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
          .build()));
    }

    // If a snapshot is currently being received and the snapshot versions don't match, simply
    // close the existing snapshot. This is a naive implementation that assumes that the leader
    // will be responsible in sending the correct snapshot to this server. Leaders must dictate
    // where snapshots must be sent since entries can still legitimately exist prior to the snapshot,
    // and so snapshots aren't simply sent at the beginning of the follower's log, but rather the
    // leader dictates when a snapshot needs to be sent.
    if (pendingSnapshot != null && request.index() != pendingSnapshot.index()) {
      pendingSnapshot.close();
      pendingSnapshot.delete();
      pendingSnapshot = null;
      nextSnapshotOffset = 0;
    }

    // If there is no pending snapshot, create a new snapshot.
    if (pendingSnapshot == null) {
      // For new snapshots, the initial snapshot offset must be 0.
      if (request.offset() > 0) {
        return CompletableFuture.completedFuture(logResponse(
          InstallResponse.builder()
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
            .build()));
      }

      pendingSnapshot = context.getSnapshotStore().createSnapshot(request.index());
      nextSnapshotOffset = 0;
    }

    // If the request offset is greater than the next expected snapshot offset, fail the request.
    if (request.offset() > nextSnapshotOffset) {
      return CompletableFuture.completedFuture(logResponse(
        InstallResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
          .build()));
    }

    // Write the data to the snapshot.
    try (SnapshotWriter writer = pendingSnapshot.writer()) {
      writer.write(request.data());
    }

    // If the snapshot is complete, store the snapshot and reset state, otherwise update the next snapshot offset.
    if (request.complete()) {
      pendingSnapshot.complete();
      pendingSnapshot = null;
      nextSnapshotOffset = 0;
    } else {
      nextSnapshotOffset++;
    }

    return CompletableFuture.completedFuture(logResponse(
      InstallResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .build()));
  }

  @Override
  public CompletableFuture<Void> close() {
    if (pendingSnapshot != null) {
      pendingSnapshot.close();
      pendingSnapshot.delete();
      pendingSnapshot = null;
    }
    return super.close();
  }

}
