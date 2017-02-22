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

import io.atomix.copycat.protocol.request.ProtocolRequest;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.protocol.request.AppendRequest;
import io.atomix.copycat.server.protocol.request.PollRequest;
import io.atomix.copycat.server.protocol.request.VoteRequest;
import io.atomix.copycat.server.protocol.response.AppendResponse;
import io.atomix.copycat.server.protocol.response.PollResponse;
import io.atomix.copycat.server.protocol.response.VoteResponse;
import io.atomix.copycat.server.storage.Indexed;
import io.atomix.copycat.server.storage.LogReader;
import io.atomix.copycat.server.storage.LogWriter;
import io.atomix.copycat.server.storage.entry.ConnectEntry;
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Abstract active state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class ActiveState extends PassiveState {

  protected ActiveState(ServerContext context) {
    super(context);
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(final AppendRequest request) {
    context.checkThread();
    logRequest(request);

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and transition to follower.
    boolean transition = updateTermAndLeader(request.term(), request.leader());

    CompletableFuture<AppendResponse> future = CompletableFuture.completedFuture(logResponse(handleAppend(request)));

    // If a transition is required then transition back to the follower state.
    // If the node is already a follower then the transition will be ignored.
    if (transition) {
      context.transition(CopycatServer.State.FOLLOWER);
    }
    return future;
  }

  @Override
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
  public CompletableFuture<PollResponse> onPoll(PollRequest request) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), 0);
    return CompletableFuture.completedFuture(logResponse(handlePoll(request)));
  }

  /**
   * Handles a poll request.
   */
  protected PollResponse handlePoll(PollRequest request) {
    // If the request term is not as great as the current context term then don't
    // vote for the candidate. We want to vote for candidates that are at least
    // as up to date as us.
    if (request.term() < context.getTerm()) {
      LOGGER.debug("{} - Rejected {}: candidate's term is less than the current term", context.getCluster().member().address(), request);
      return PollResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withAccepted(false)
        .build();
    } else if (isLogUpToDate(request.logIndex(), request.logTerm(), request)) {
      return PollResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withAccepted(true)
        .build();
    } else {
      return PollResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withAccepted(false)
        .build();
    }
  }

  @Override
  public CompletableFuture<VoteResponse> onVote(VoteRequest request) {
    context.checkThread();
    logRequest(request);

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context.
    boolean transition = updateTermAndLeader(request.term(), 0);

    CompletableFuture<VoteResponse> future = CompletableFuture.completedFuture(logResponse(handleVote(request)));
    if (transition) {
      context.transition(CopycatServer.State.FOLLOWER);
    }
    return future;
  }

  /**
   * Handles a vote request.
   */
  protected VoteResponse handleVote(VoteRequest request) {
    // If the request term is not as great as the current context term then don't
    // vote for the candidate. We want to vote for candidates that are at least
    // as up to date as us.
    if (request.term() < context.getTerm()) {
      LOGGER.debug("{} - Rejected {}: candidate's term is less than the current term", context.getCluster().member().address(), request);
      return VoteResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If a leader was already determined for this term then reject the request.
    else if (context.getLeader() != null) {
      LOGGER.debug("{} - Rejected {}: leader already exists", context.getCluster().member().address(), request);
      return VoteResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If the requesting candidate is not a known member of the cluster (to this
    // node) then don't vote for it. Only vote for candidates that we know about.
    else if (!context.getClusterState().getRemoteMemberStates().stream().<Integer>map(m -> m.getMember().id()).collect(Collectors.toSet()).contains(request.candidate())) {
      LOGGER.debug("{} - Rejected {}: candidate is not known to the local member", context.getCluster().member().address(), request);
      return VoteResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If no vote has been cast, check the log and cast a vote if necessary.
    else if (context.getLastVotedFor() == 0) {
      if (isLogUpToDate(request.logIndex(), request.logTerm(), request)) {
        context.setLastVotedFor(request.candidate());
        return VoteResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(true)
          .build();
      } else {
        return VoteResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(false)
          .build();
      }
    }
    // If we already voted for the requesting server, respond successfully.
    else if (context.getLastVotedFor() == request.candidate()) {
      LOGGER.debug("{} - Accepted {}: already voted for {}", context.getCluster().member().address(), request, context.getCluster().member(context.getLastVotedFor()).address());
      return VoteResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(true)
        .build();
    }
    // In this case, we've already voted for someone else.
    else {
      LOGGER.debug("{} - Rejected {}: already voted for {}", context.getCluster().member().address(), request, context.getCluster().member(context.getLastVotedFor()).address());
      return VoteResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
  }

  /**
   * Returns a boolean value indicating whether the given candidate's log is up-to-date.
   */
  boolean isLogUpToDate(long lastIndex, long lastTerm, ProtocolRequest request) {
    // Read the last entry from the log.
    final Indexed<?> lastEntry = context.getLogWriter().lastEntry();

    // If the log is empty then vote for the candidate.
    if (lastEntry == null) {
      LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getCluster().member().address(), request);
      return true;
    }

    // If the candidate's last log term is lower than the local log's last entry term, reject the request.
    if (lastTerm < lastEntry.term()) {
      LOGGER.debug("{} - Rejected {}: candidate's last log entry ({}) is at a lower term than the local log ({})", context.getCluster().member().address(), request, lastTerm, lastEntry.term());
      return false;
    }

    // If the candidate's last term is equal to the local log's last entry term, reject the request if the
    // candidate's last index is less than the local log's last index. If the candidate's last log term is
    // greater than the local log's last term then it's considered up to date, and if both have the same term
    // then the candidate's last index must be greater than the local log's last index.
    if (lastTerm == lastEntry.term() && lastIndex < lastEntry.index()) {
      LOGGER.debug("{} - Rejected {}: candidate's last log entry ({}) is at a lower index than the local log ({})", context.getCluster().member().address(), request, lastIndex, lastEntry.index());
      return false;
    }

    // If we made it this far, the candidate's last term is greater than or equal to the local log's last
    // term, and if equal to the local log's last term, the candidate's last index is equal to or greater
    // than the local log's last index.
    LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getCluster().member().address(), request);
    return true;
  }

}
