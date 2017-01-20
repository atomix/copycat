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

import io.atomix.copycat.Query;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.error.CopycatException;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.protocol.request.ConnectRequest;
import io.atomix.copycat.protocol.request.QueryRequest;
import io.atomix.copycat.protocol.response.ConnectResponse;
import io.atomix.copycat.protocol.response.OperationResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.response.QueryResponse;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.request.AppendRequest;
import io.atomix.copycat.server.protocol.request.InstallRequest;
import io.atomix.copycat.server.protocol.response.AppendResponse;
import io.atomix.copycat.server.protocol.response.InstallResponse;
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
      context.getLog().truncate(Math.min(context.getCommitIndex(), context.getLog().lastIndex()));
    }
  }

  @Override
  public CompletableFuture<ConnectResponse> onConnect(ConnectRequest request, ConnectResponse.Builder builder, ProtocolServerConnection connection) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(
        builder.withStatus(ProtocolResponse.Status.ERROR)
          .withError(CopycatError.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      // Immediately register the session connection and send an accept request to the leader.
      context.getStateMachine().executor().context().sessions().registerConnection(request.client(), connection);

      return this.forward(c -> c.accept(b ->
        b.withClient(request.client())
          .withAddress(context.getCluster().member().serverAddress())
          .build()))
        .thenApply(acceptResponse ->
          builder.withStatus(ProtocolResponse.Status.OK)
            .withLeader(context.getLeader() != null ? context.getLeader().clientAddress() : null)
            .withMembers(context.getCluster().members().stream()
              .map(Member::clientAddress)
              .filter(m -> m != null)
              .collect(Collectors.toList()))
            .build())
        .exceptionally(error ->
          builder.withStatus(ProtocolResponse.Status.ERROR)
            .withError(CopycatError.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(final AppendRequest request, AppendResponse.Builder builder) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());

    return CompletableFuture.completedFuture(logResponse(handleAppend(request, builder)));
  }

  /**
   * Handles an append request.
   */
  protected AppendResponse handleAppend(AppendRequest request, AppendResponse.Builder builder) {
    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.getTerm()) {
      LOGGER.debug("{} - Rejected {}: request term is less than the current term ({})", context.getCluster().member().address(), request, context.getTerm());
      return builder.withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    } else {
      return checkGlobalIndex(request, builder);
    }
  }

  /**
   * Checks whether the log needs to be truncated based on the globalIndex.
   */
  protected AppendResponse checkGlobalIndex(AppendRequest request, AppendResponse.Builder builder) {
    // If the globalIndex has changed and is not present in the local log, truncate the log.
    // This ensures that if major compaction progressed on any server beyond the entries in this
    // server's log that this server receives all entries after compaction.
    // If the current global index is 0 then do not perform the index check. This ensures that
    // servers don't truncate their logs at startup.
    // Ensure that the globalIndex is updated here to prevent endlessly truncating the log
    // if the AppendRequest is rejected.
    long currentGlobalIndex = context.getGlobalIndex();
    long nextGlobalIndex = request.globalIndex();
    if (currentGlobalIndex > 0 && nextGlobalIndex > currentGlobalIndex && nextGlobalIndex > context.getLog().lastIndex()) {
      context.setGlobalIndex(nextGlobalIndex);
      context.reset();
    }

    // If an entry was provided, check the entry against the local log.
    if (request.logIndex() != 0) {
      return checkPreviousEntry(request, builder);
    } else {
      return appendEntries(request, builder);
    }
  }

  /**
   * Checks the previous entry in the append request for consistency.
   */
  protected AppendResponse checkPreviousEntry(AppendRequest request, AppendResponse.Builder builder) {
    if (request.logIndex() != 0 && context.getLog().isEmpty()) {
      LOGGER.debug("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getCluster().member().address(), request, request.logIndex(), context.getLog().lastIndex());
      return builder.withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    } else if (request.logIndex() != 0 && context.getLog().lastIndex() != 0 && request.logIndex() > context.getLog().lastIndex()) {
      LOGGER.debug("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getCluster().member().address(), request, request.logIndex(), context.getLog().lastIndex());
      return builder.withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    }
    return appendEntries(request, builder);
  }

  /**
   * Appends entries to the local log.
   */
  protected AppendResponse appendEntries(AppendRequest request, AppendResponse.Builder builder) {
    // Get the last entry index or default to the request log index.
    long lastEntryIndex = request.logIndex();
    if (!request.entries().isEmpty()) {
      lastEntryIndex = request.entries().get(request.entries().size() - 1).getIndex();
    }

    // Ensure the commitIndex is not increased beyond the index of the last entry in the request.
    long commitIndex = Math.max(context.getCommitIndex(), Math.min(request.commitIndex(), lastEntryIndex));

    // Append entries to the log starting at the last log index.
    for (Entry entry : request.entries()) {
      // If the entry index is greater than the last index and less than the commit index, append the entry.
      // We perform no additional consistency checks here since passive members may only receive committed entries.
      if (context.getLog().lastIndex() < entry.getIndex() && entry.getIndex() <= commitIndex) {
        context.getLog().skip(entry.getIndex() - context.getLog().lastIndex() - 1).append(entry);
        LOGGER.debug("{} - Appended {} to log at index {}", context.getCluster().member().address(), entry, entry.getIndex());
      }

      // If the entry is a connect entry then immediately configure the connection.
      if (entry instanceof ConnectEntry) {
        ConnectEntry connectEntry = (ConnectEntry) entry;
        context.getStateMachine().executor().context().sessions().registerAddress(connectEntry.client(), connectEntry.address());
      }
    }

    // Update the context commit and global indices.
    context.setCommitIndex(commitIndex);
    context.setGlobalIndex(request.globalIndex());

    // Apply commits to the state machine in batch.
    context.getStateMachine().applyAll(context.getCommitIndex());

    return builder.withStatus(ProtocolResponse.Status.OK)
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .withLogIndex(context.getLog().lastIndex())
      .build();
  }

  @Override
  public CompletableFuture<QueryResponse> onQuery(QueryRequest request, QueryResponse.Builder builder) {
    context.checkThread();
    logRequest(request);

    // If the query was submitted with RYW or monotonic read consistency, attempt to apply the query to the local state machine.
    if (request.query().consistency() == Query.ConsistencyLevel.SEQUENTIAL) {

      // If this server has not yet applied entries up to the client's session ID, forward the
      // query to the leader. This ensures that a follower does not tell the client its session
      // doesn't exist if the follower hasn't had a chance to see the session's registration entry.
      if (context.getStateMachine().getLastApplied() < request.session()) {
        LOGGER.debug("{} - State out of sync, forwarding query to leader");
        return queryForward(request, builder);
      }

      // If the commit index is not in the log then we've fallen too far behind the leader to perform a local query.
      // Forward the request to the leader.
      if (context.getLog().lastIndex() < context.getCommitIndex()) {
        LOGGER.debug("{} - State out of sync, forwarding query to leader");
        return queryForward(request, builder);
      }

      QueryEntry entry = context.getLog().create(QueryEntry.class)
        .setIndex(request.index())
        .setTerm(context.getTerm())
        .setTimestamp(System.currentTimeMillis())
        .setSession(request.session())
        .setSequence(request.sequence())
        .setQuery(request.query());

      return queryLocal(entry, builder);
    } else {
      return queryForward(request, builder);
    }
  }

  /**
   * Forwards the query to the leader.
   */
  private CompletableFuture<QueryResponse> queryForward(QueryRequest request, QueryResponse.Builder builder) {
    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(
        builder.withStatus(ProtocolResponse.Status.ERROR)
          .withError(CopycatError.Type.NO_LEADER_ERROR)
          .build()));
    }

    LOGGER.debug("{} - Forwarded {}", context.getCluster().member().address(), request);
    return forward(connection -> connection.query(b ->
      b.withSession(request.session())
        .withSequence(request.sequence())
        .withIndex(request.index())
        .withQuery(request.query())
        .build()))
      .exceptionally(error ->
        builder.withStatus(ProtocolResponse.Status.ERROR)
          .withError(CopycatError.Type.NO_LEADER_ERROR)
          .build())
      .thenApply(this::logResponse);
  }

  /**
   * Performs a local query.
   */
  protected CompletableFuture<QueryResponse> queryLocal(QueryEntry entry, QueryResponse.Builder builder) {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    sequenceQuery(entry, builder, future);
    return future;
  }

  /**
   * Sequences the given query.
   */
  private void sequenceQuery(QueryEntry entry, QueryResponse.Builder builder, CompletableFuture<QueryResponse> future) {
    // Get the client's server session. If the session doesn't exist, return an unknown session error.
    ServerSessionContext session = context.getStateMachine().executor().context().sessions().getSession(entry.session());
    if (session == null) {
      future.complete(logResponse(
        builder.withStatus(ProtocolResponse.Status.ERROR)
          .withError(CopycatError.Type.UNKNOWN_SESSION_ERROR)
          .build()));
    } else {
      sequenceQuery(entry, builder, session, future);
    }
  }

  /**
   * Sequences the given query.
   */
  private void sequenceQuery(QueryEntry entry, QueryResponse.Builder builder, ServerSessionContext session, CompletableFuture<QueryResponse> future) {
    // If the query's sequence number is greater than the session's current sequence number, queue the request for
    // handling once the state machine is caught up.
    if (entry.sequence() > session.getCommandSequence()) {
      session.registerSequenceQuery(entry.sequence(), () -> indexQuery(entry, builder, future));
    } else {
      indexQuery(entry, builder, future);
    }
  }

  /**
   * Ensures the given query is applied after the appropriate index.
   */
  private void indexQuery(QueryEntry entry, QueryResponse.Builder builder, CompletableFuture<QueryResponse> future) {
    // Get the client's server session. If the session doesn't exist, return an unknown session error.
    ServerSessionContext session = context.getStateMachine().executor().context().sessions().getSession(entry.session());
    if (session == null) {
      future.complete(logResponse(
        builder.withStatus(ProtocolResponse.Status.ERROR)
          .withError(CopycatError.Type.UNKNOWN_SESSION_ERROR)
          .build()));
    } else {
      indexQuery(entry, builder, session, future);
    }
  }

  /**
   * Ensures the given query is applied after the appropriate index.
   */
  private void indexQuery(QueryEntry entry, QueryResponse.Builder builder, ServerSessionContext session, CompletableFuture<QueryResponse> future) {
    // If the query index is greater than the session's last applied index, queue the request for handling once the
    // state machine is caught up.
    if (entry.getIndex() > session.getLastApplied()) {
      session.registerIndexQuery(entry.getIndex(), () -> applyQuery(entry, builder, future));
    } else {
      applyQuery(entry, builder, future);
    }
  }

  /**
   * Applies a query to the state machine.
   */
  protected CompletableFuture<QueryResponse> applyQuery(QueryEntry entry, QueryResponse.Builder builder, CompletableFuture<QueryResponse> future) {
    // In the case of the leader, the state machine is always up to date, so no queries will be queued and all query
    // indexes will be the last applied index.
    context.getStateMachine().<ServerStateMachine.Result>apply(entry).whenComplete((result, error) -> {
      completeOperation(result, builder, error, future);
      entry.release();
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
        if (result.result instanceof Exception) {
          error = (Exception) result.result;
        }
      }

      if (error == null) {
        future.complete(logResponse(builder.withStatus(ProtocolResponse.Status.OK)
          .withResult(result.result)
          .build()));
      } else if (error instanceof CompletionException && error.getCause() instanceof CopycatException) {
        future.complete(logResponse(builder.withStatus(ProtocolResponse.Status.ERROR)
          .withError(((CopycatException) error.getCause()).getType())
          .build()));
      } else if (error instanceof CopycatException) {
        future.complete(logResponse(builder.withStatus(ProtocolResponse.Status.ERROR)
          .withError(((CopycatException) error).getType())
          .build()));
      } else {
        future.complete(logResponse(builder.withStatus(ProtocolResponse.Status.ERROR)
          .withError(CopycatError.Type.INTERNAL_ERROR)
          .build()));
      }
    }
  }

  @Override
  public CompletableFuture<InstallResponse> onInstall(InstallRequest request, InstallResponse.Builder builder) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());

    // If the request is for a lesser term, reject the request.
    if (request.term() < context.getTerm()) {
      return CompletableFuture.completedFuture(logResponse(
        builder.withStatus(ProtocolResponse.Status.ERROR)
          .withError(CopycatError.Type.ILLEGAL_MEMBER_STATE_ERROR)
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
          builder.withStatus(ProtocolResponse.Status.ERROR)
            .withError(CopycatError.Type.ILLEGAL_MEMBER_STATE_ERROR)
            .build()));
      }

      pendingSnapshot = context.getSnapshotStore().createSnapshot(request.index());
      nextSnapshotOffset = 0;
    }

    // If the request offset is greater than the next expected snapshot offset, fail the request.
    if (request.offset() > nextSnapshotOffset) {
      return CompletableFuture.completedFuture(logResponse(
        builder.withStatus(ProtocolResponse.Status.ERROR)
          .withError(CopycatError.Type.ILLEGAL_MEMBER_STATE_ERROR)
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
      builder.withStatus(ProtocolResponse.Status.OK)
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
