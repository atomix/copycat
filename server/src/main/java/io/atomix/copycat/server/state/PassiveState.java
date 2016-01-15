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

import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.error.RaftError;
import io.atomix.copycat.client.error.RaftException;
import io.atomix.copycat.client.request.QueryRequest;
import io.atomix.copycat.client.response.QueryResponse;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.request.InstallRequest;
import io.atomix.copycat.server.response.AppendResponse;
import io.atomix.copycat.server.response.InstallResponse;
import io.atomix.copycat.server.storage.entry.ConnectEntry;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.server.storage.entry.QueryEntry;
import io.atomix.copycat.server.storage.snapshot.Snapshot;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;

import java.util.concurrent.CompletableFuture;

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
  public CompletableFuture<AbstractState> open() {
    return super.open().thenRun(this::truncateUncommittedEntries).thenApply(v -> this);
  }

  /**
   * Truncates uncommitted entries from the log.
   */
  private void truncateUncommittedEntries() {
    context.getLog().truncate(Math.min(context.getCommitIndex(), context.getLog().lastIndex()));
  }

  @Override
  protected CompletableFuture<AppendResponse> append(final AppendRequest request) {
    context.checkThread();

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm() || (request.term() == context.getTerm() && context.getLeader() == null)) {
      context.setTerm(request.term());
      context.setLeader(request.leader());
    }

    return CompletableFuture.completedFuture(logResponse(handleAppend(logRequest(request))));
  }

  /**
   * Handles an append request.
   */
  private AppendResponse handleAppend(AppendRequest request) {
    if (request.logIndex() > 0) {
      return checkPreviousEntry(request);
    } else {
      return appendEntries(request);
    }
  }

  /**
   * Checks the previous entry in the append request for consistency.
   */
  private AppendResponse checkPreviousEntry(AppendRequest request) {
    if (request.logIndex() != 0 && context.getLog().isEmpty()) {
      LOGGER.debug("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getCluster().member().address(), request, request.logIndex(), context.getLog().lastIndex());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    } else if (request.logIndex() != 0 && context.getLog().lastIndex() != 0 && request.logIndex() > context.getLog().lastIndex()) {
      LOGGER.debug("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getCluster().member().address(), request, request.logIndex(), context.getLog().lastIndex());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    }
    return appendEntries(request);
  }

  /**
   * Appends entries to the local log.
   */
  private AppendResponse appendEntries(AppendRequest request) {
    // Append entries to the log starting at the last log index.
    long commitIndex = Math.max(context.getCommitIndex(), request.commitIndex());
    for (Entry entry : request.entries()) {
      // If the entry index is greater than the last index and less than the commit index, append the entry.
      // We perform no additional consistency checks here since passive members may only receive committed entries.
      if (context.getLog().lastIndex() < entry.getIndex() && entry.getIndex() < commitIndex) {
        context.getLog().skip(entry.getIndex() - context.getLog().lastIndex() - 1).append(entry);
        LOGGER.debug("{} - Appended {} to log at index {}", context.getCluster().member().address(), entry, entry.getIndex());
      }

      // If the entry is a connect entry then immediately configure the connection.
      if (entry instanceof ConnectEntry) {
        ConnectEntry connectEntry = (ConnectEntry) entry;
        context.getStateMachine().executor().context().sessions().registerAddress(connectEntry.getClient(), connectEntry.getAddress());
      }
    }

    // If we've made it this far, apply commits and send a successful response.
    // Apply commits to the state machine asynchronously so the append request isn't blocked on I/O.
    context.setCommitIndex(commitIndex);
    context.getThreadContext().execute(() -> context.getStateMachine().applyAll(commitIndex));

    return AppendResponse.builder()
      .withStatus(Response.Status.OK)
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .withLogIndex(context.getLog().lastIndex())
      .build();
  }

  @Override
  protected CompletableFuture<QueryResponse> query(QueryRequest request) {
    context.checkThread();
    logRequest(request);

    // If the query was submitted with RYW or monotonic read consistency, attempt to apply the query to the local state machine.
    if (request.query().consistency() == Query.ConsistencyLevel.CAUSAL
      || request.query().consistency() == Query.ConsistencyLevel.SEQUENTIAL) {

      // If this server has not yet applied entries up to the client's session ID, forward the
      // query to the leader. This ensures that a follower does not tell the client its session
      // doesn't exist if the follower hasn't had a chance to see the session's registration entry.
      if (context.getStateMachine().getLastApplied() < request.session()) {
        LOGGER.debug("{} - State out of sync, forwarding query to leader");
        return queryForward(request);
      }

      // If the commit index is not in the log then we've fallen too far behind the leader to perform a local query.
      // Forward the request to the leader.
      if (context.getLog().lastIndex() < context.getCommitIndex()) {
        LOGGER.debug("{} - State out of sync, forwarding query to leader");
        return queryForward(request);
      }

      return queryLocal(request);
    } else {
      return queryForward(request);
    }
  }

  /**
   * Forwards the query to the leader.
   */
  private CompletableFuture<QueryResponse> queryForward(QueryRequest request) {
    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    }

    LOGGER.debug("{} - Forwarded {}", context.getCluster().member().address(), request);
    return this.<QueryRequest, QueryResponse>forward(request).thenApply(this::logResponse);
  }

  /**
   * Performs a local query.
   */
  private CompletableFuture<QueryResponse> queryLocal(QueryRequest request) {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();

    QueryEntry entry = context.getLog().create(QueryEntry.class)
      .setIndex(request.index())
      .setTerm(context.getTerm())
      .setTimestamp(System.currentTimeMillis())
      .setSession(request.session())
      .setSequence(request.sequence())
      .setQuery(request.query());

    // For CAUSAL queries, the state machine version is the last index applied to the state machine. For other consistency
    // levels, the state machine may actually wait until those queries are applied to the state machine, so the last applied
    // index is not necessarily the index at which the query will be applied, but it will be applied after its sequence.
    final long index;
    if (request.query().consistency() == Query.ConsistencyLevel.CAUSAL) {
      index = context.getStateMachine().getLastApplied();
    } else {
      index = Math.max(request.sequence(), context.getStateMachine().getLastApplied());
    }

    context.getStateMachine().apply(entry).whenCompleteAsync((result, error) -> {
      if (isOpen()) {
        if (error == null) {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.OK)
            .withIndex(index)
            .withResult(result)
            .build()));
        } else if (error instanceof RaftException) {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withIndex(index)
            .withError(((RaftException) error).getType())
            .build()));
        } else {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withIndex(index)
            .withError(RaftError.Type.INTERNAL_ERROR)
            .build()));
        }
      }
      entry.release();
    }, context.getThreadContext().executor());
    return future;
  }

  @Override
  protected CompletableFuture<InstallResponse> install(InstallRequest request) {
    context.checkThread();
    logRequest(request);

    // If the request is for a lesser term, reject the request.
    if (request.term() < context.getTerm()) {
      return CompletableFuture.completedFuture(logResponse(InstallResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
        .build()));
    }

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm() || (request.term() == context.getTerm() && context.getLeader() == null)) {
      context.setTerm(request.term());
      context.setLeader(request.leader());
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
        return CompletableFuture.completedFuture(logResponse(InstallResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
          .build()));
      }

      pendingSnapshot = context.getSnapshotStore().createSnapshot(request.index());
      nextSnapshotOffset = 0;
    }

    // If the request offset is greater than the next expected snapshot offset, fail the request.
    if (request.offset() > nextSnapshotOffset) {
      return CompletableFuture.completedFuture(logResponse(InstallResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
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

    return CompletableFuture.completedFuture(logResponse(InstallResponse.builder()
      .withStatus(Response.Status.OK)
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
