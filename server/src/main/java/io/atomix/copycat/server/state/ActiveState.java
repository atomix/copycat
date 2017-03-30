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

import io.atomix.copycat.protocol.Request;
import io.atomix.copycat.protocol.Response;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.protocol.*;
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
  public CompletableFuture<AppendResponse> append(final AppendRequest request) {
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
  protected AppendResponse checkPreviousEntry(AppendRequest request) {
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

    // If the previous entry term doesn't match the local previous term then reject the request.
    long term = context.getLog().term(request.logIndex());
    if (term == 0 || term != request.logTerm()) {
      LOGGER.debug("{} - Rejected {}: Request log term does not match local log term {} for the same entry", context.getCluster().member().address(), request, term);
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(request.logIndex() <= context.getLog().lastIndex() ? request.logIndex() - 1 : context.getLog().lastIndex())
        .build();
    } else {
      return appendEntries(request);
    }
  }

  @Override
  protected AppendResponse appendEntries(AppendRequest request) {
    // Get the last entry index or default to the request log index.
    long lastEntryIndex = request.logIndex();
    if (!request.entries().isEmpty()) {
      lastEntryIndex = request.entries().get(request.entries().size() - 1).getIndex();
    }

    // Ensure the commitIndex is not increased beyond the index of the last entry in the request.
    long commitIndex = Math.max(context.getCommitIndex(), Math.min(request.commitIndex(), lastEntryIndex));

    // Iterate through request entries and append them to the log.
    for (Entry entry : request.entries()) {
      // If the entry index is greater than the last log index, skip missing entries.
      if (context.getLog().lastIndex() < entry.getIndex()) {
        context.getLog().skip(entry.getIndex() - context.getLog().lastIndex() - 1).append(entry);
        LOGGER.trace("{} - Appended {} to log at index {}", context.getCluster().member().address(), entry, entry.getIndex());
      } else if (entry.getIndex() > context.getCommitIndex()) {
        // Compare the term of the received entry with the matching entry in the log.
        long term = context.getLog().term(entry.getIndex());
        if (term != 0) {
          if (entry.getTerm() != term) {
            // We found an invalid entry in the log. Remove the invalid entry and append the new entry.
            // If appending to the log fails, apply commits and reply false to the append request.
            LOGGER.debug("{} - Appended entry term does not match local log, removing incorrect entries", context.getCluster().member().address());
            context.getLog().truncate(entry.getIndex() - 1).append(entry);
            LOGGER.trace("{} - Appended {} to log at index {}", context.getCluster().member().address(), entry, entry.getIndex());
          }
        } else {
          context.getLog().truncate(entry.getIndex() - 1).append(entry);
          LOGGER.trace("{} - Appended {} to log at index {}", context.getCluster().member().address(), entry, entry.getIndex());
        }
      }
    }

    // If we've made it this far, apply commits and send a successful response.
    long previousCommitIndex = context.getCommitIndex();
    context.setCommitIndex(commitIndex);
    context.setGlobalIndex(request.globalIndex());

    if (context.getCommitIndex() > previousCommitIndex) {
      LOGGER.trace("{} - Committed entries up to index {}", context.getCluster().member().address(), commitIndex);
    }

    // Apply commits to the local state machine.
    context.getStateMachine().applyAll(context.getCommitIndex());

    return AppendResponse.builder()
      .withStatus(Response.Status.OK)
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .withLogIndex(context.getLog().lastIndex())
      .build();
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
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
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withAccepted(false)
        .build();
    } else if (isLogUpToDate(request.logIndex(), request.logTerm(), request)) {
      return PollResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withAccepted(true)
        .build();
    } else {
      return PollResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withAccepted(false)
        .build();
    }
  }

  @Override
  public CompletableFuture<VoteResponse> vote(VoteRequest request) {
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
      LOGGER.trace("{} - Rejected {}: candidate's term is less than the current term", context.getCluster().member().address(), request);
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If a leader was already determined for this term then reject the request.
    else if (context.getLeader() != null) {
      LOGGER.trace("{} - Rejected {}: leader already exists", context.getCluster().member().address(), request);
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If the requesting candidate is not a known member of the cluster (to this
    // node) then don't vote for it. Only vote for candidates that we know about.
    else if (!context.getClusterState().getRemoteMemberStates().stream().<Integer>map(m -> m.getMember().id()).collect(Collectors.toSet()).contains(request.candidate())) {
      LOGGER.trace("{} - Rejected {}: candidate is not known to the local member", context.getCluster().member().address(), request);
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If no vote has been cast, check the log and cast a vote if necessary.
    else if (context.getLastVotedFor() == 0) {
      if (isLogUpToDate(request.logIndex(), request.logTerm(), request)) {
        context.setLastVotedFor(request.candidate());
        return VoteResponse.builder()
          .withStatus(Response.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(true)
          .build();
      } else {
        return VoteResponse.builder()
          .withStatus(Response.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(false)
          .build();
      }
    }
    // If we already voted for the requesting server, respond successfully.
    else if (context.getLastVotedFor() == request.candidate()) {
      LOGGER.debug("{} - Accepted {}: already voted for {}", context.getCluster().member().address(), request, context.getCluster().member(context.getLastVotedFor()).address());
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(true)
        .build();
    }
    // In this case, we've already voted for someone else.
    else {
      LOGGER.debug("{} - Rejected {}: already voted for {}", context.getCluster().member().address(), request, context.getCluster().member(context.getLastVotedFor()).address());
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
  }

  /**
   * Returns a boolean value indicating whether the given candidate's log is up-to-date.
   */
  boolean isLogUpToDate(long lastIndex, long lastTerm, Request request) {
    // If the log is empty then vote for the candidate.
    if (context.getLog().isEmpty()) {
      LOGGER.trace("{} - Accepted {}: candidate's log is up-to-date", context.getCluster().member().address(), request);
      return true;
    }

    // Read the last entry index and term from the log.
    long localLastIndex = context.getLog().lastIndex();
    long localLastTerm = context.getLog().term(localLastIndex);

    // If the candidate's last log term is lower than the local log's last entry term, reject the request.
    if (lastTerm < localLastTerm) {
      LOGGER.trace("{} - Rejected {}: candidate's last log entry ({}) is at a lower term than the local log ({})", context.getCluster().member().address(), request, lastTerm, localLastTerm);
      return false;
    }

    // If the candidate's last term is equal to the local log's last entry term, reject the request if the
    // candidate's last index is less than the local log's last index. If the candidate's last log term is
    // greater than the local log's last term then it's considered up to date, and if both have the same term
    // then the candidate's last index must be greater than the local log's last index.
    if (lastTerm == localLastTerm && lastIndex < localLastIndex) {
      LOGGER.trace("{} - Rejected {}: candidate's last log entry ({}) is at a lower index than the local log ({})", context.getCluster().member().address(), request, lastIndex, localLastIndex);
      return false;
    }

    // If we made it this far, the candidate's last term is greater than or equal to the local log's last
    // term, and if equal to the local log's last term, the candidate's last index is equal to or greater
    // than the local log's last index.
    LOGGER.trace("{} - Accepted {}: candidate's log is up-to-date", context.getCluster().member().address(), request);
    return true;
  }

}
