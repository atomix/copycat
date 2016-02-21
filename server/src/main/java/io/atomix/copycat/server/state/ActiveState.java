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
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.AppendRequest;
import io.atomix.copycat.server.protocol.PollRequest;
import io.atomix.copycat.server.protocol.VoteRequest;
import io.atomix.copycat.server.protocol.AppendResponse;
import io.atomix.copycat.server.protocol.PollResponse;
import io.atomix.copycat.server.protocol.VoteResponse;
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
  protected CompletableFuture<AppendResponse> append(final AppendRequest request) {
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
    // If the log contains entries after the request's previous log index
    // then remove those entries to be replaced by the request entries.
    if (!request.entries().isEmpty()) {

      // Iterate through request entries and append them to the log.
      for (Entry entry : request.entries()) {
        // If the entry index is greater than the last log index, skip missing entries.
        if (context.getLog().lastIndex() < entry.getIndex()) {
          context.getLog().skip(entry.getIndex() - context.getLog().lastIndex() - 1).append(entry);
          LOGGER.debug("{} - Appended {} to log at index {}", context.getCluster().member().address(), entry, entry.getIndex());
        } else if (context.getCommitIndex() >= entry.getIndex()) {
          continue;
        } else {
          // Compare the term of the received entry with the matching entry in the log.
          long term = context.getLog().term(entry.getIndex());
          if (term != 0) {
            if (entry.getTerm() != term) {
              // We found an invalid entry in the log. Remove the invalid entry and append the new entry.
              // If appending to the log fails, apply commits and reply false to the append request.
              LOGGER.debug("{} - Appended entry term does not match local log, removing incorrect entries", context.getCluster().member().address());
              context.getLog().truncate(entry.getIndex() - 1).append(entry);
              LOGGER.debug("{} - Appended {} to log at index {}", context.getCluster().member().address(), entry, entry.getIndex());
            }
          } else {
            context.getLog().truncate(entry.getIndex() - 1).append(entry);
            LOGGER.debug("{} - Appended {} to log at index {}", context.getCluster().member().address(), entry, entry.getIndex());
          }
        }

        // If the entry is a connect entry then immediately configure the connection.
        if (entry instanceof ConnectEntry) {
          ConnectEntry connectEntry = (ConnectEntry) entry;
          context.getStateMachine().executor().context().sessions().registerAddress(connectEntry.getClient(), connectEntry.getAddress());
        }
      }
    }

    // If we've made it this far, apply commits and send a successful response.
    context.setCommitIndex(request.commitIndex());
    context.setGlobalIndex(request.globalIndex());

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
  protected CompletableFuture<PollResponse> poll(PollRequest request) {
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
  protected CompletableFuture<VoteResponse> vote(VoteRequest request) {
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
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If a leader was already determined for this term then reject the request.
    else if (context.getLeader() != null) {
      LOGGER.debug("{} - Rejected {}: leader already exists", context.getCluster().member().address(), request);
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If the requesting candidate is not a known member of the cluster (to this
    // node) then don't vote for it. Only vote for candidates that we know about.
    else if (!context.getCluster().members().stream().<Integer>map(Member::id).collect(Collectors.toSet()).contains(request.candidate())) {
      LOGGER.debug("{} - Rejected {}: candidate is not known to the local member", context.getCluster().member().address(), request);
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
  boolean isLogUpToDate(long index, long term, Request request) {
    // If the log is empty then vote for the candidate.
    if (context.getLog().isEmpty()) {
      LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getCluster().member().address(), request);
      return true;
    } else {
      // Otherwise, load the last entry in the log. The last entry should be
      // at least as up to date as the candidates entry and term.
      long lastIndex = context.getLog().lastIndex();
      long lastTerm = context.getLog().term(lastIndex);
      if (lastTerm == 0) {
        LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getCluster().member().address(), request);
        return true;
      }

      if (index != 0 && index >= lastIndex) {
        if (term >= lastTerm) {
          LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getCluster().member().address(), request);
          return true;
        } else {
          LOGGER.debug("{} - Rejected {}: candidate's last log term ({}) is in conflict with local log ({})", context.getCluster().member().address(), request, term, lastTerm);
          return false;
        }
      } else {
        LOGGER.debug("{} - Rejected {}: candidate's last log entry ({}) is at a lower index than the local log ({})", context.getCluster().member().address(), request, index, lastIndex);
        return false;
      }
    }
  }

}
