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
import io.atomix.copycat.client.request.Request;
import io.atomix.copycat.client.response.QueryResponse;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.controller.ServerStateController;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.request.PollRequest;
import io.atomix.copycat.server.request.VoteRequest;
import io.atomix.copycat.server.response.AppendResponse;
import io.atomix.copycat.server.response.PollResponse;
import io.atomix.copycat.server.response.VoteResponse;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.server.storage.entry.QueryEntry;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Abstract active state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class ActiveState extends PassiveState {

  protected ActiveState(ServerStateController controller) {
    super(controller);
  }

  @Override
  public CompletableFuture<AppendResponse> append(final AppendRequest request) {
    controller.context().checkThread();

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    boolean transition = false;
    if (request.term() > controller.context().getTerm() || (request.term() == controller.context().getTerm() && controller.context().getLeader() == null)) {
      controller.context().setTerm(request.term());
      controller.context().setLeader(request.leader());
      transition = true;
    }

    CompletableFuture<AppendResponse> future = CompletableFuture.completedFuture(logResponse(handleAppend(logRequest(request))));

    // If a transition is required then transition back to the follower state.
    // If the node is already a follower then the transition will be ignored.
    if (transition) {
      controller.transition(RaftStateType.FOLLOWER);
    }
    return future;
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    controller.context().checkThread();
    return CompletableFuture.completedFuture(logResponse(handlePoll(logRequest(request))));
  }

  /**
   * Handles a poll request.
   */
  protected PollResponse handlePoll(PollRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context.
    if (request.term() > controller.context().getTerm()) {
      controller.context().setTerm(request.term());
    }


    // If the request term is not as great as the current context term then don't
    // vote for the candidate. We want to vote for candidates that are at least
    // as up to date as us.
    if (request.term() < controller.context().getTerm()) {
      LOGGER.debug("{} - Rejected {}: candidate's term is less than the current term", controller.context().getCluster().getMember().serverAddress(), request);
      return PollResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(controller.context().getTerm())
        .withAccepted(false)
        .build();
    } else if (isLogUpToDate(request.logIndex(), request.logTerm(), request)) {
      return PollResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(controller.context().getTerm())
        .withAccepted(true)
        .build();
    } else {
      return PollResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(controller.context().getTerm())
        .withAccepted(false)
        .build();
    }
  }

  @Override
  public CompletableFuture<VoteResponse> vote(VoteRequest request) {
    controller.context().checkThread();

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context.
    boolean transition = false;
    if (request.term() > controller.context().getTerm()) {
      controller.context().setTerm(request.term());
      transition = true;
    }

    CompletableFuture<VoteResponse> future = CompletableFuture.completedFuture(logResponse(handleVote(logRequest(request))));
    if (transition) {
      controller.transition(RaftStateType.FOLLOWER);
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
    if (request.term() < controller.context().getTerm()) {
      LOGGER.debug("{} - Rejected {}: candidate's term is less than the current term", controller.context().getCluster().getMember().serverAddress(), request);
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(controller.context().getTerm())
        .withVoted(false)
        .build();
    }
    // If the requesting candidate is not a known member of the cluster (to this
    // node) then don't vote for it. Only vote for candidates that we know about.
    else if (!controller.context().getCluster().getMembers().stream().<Integer>map(Member::id).collect(Collectors.toSet()).contains(request.candidate())) {
      LOGGER.debug("{} - Rejected {}: candidate is not known to the local member", controller.context().getCluster().getMember().serverAddress(), request);
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(controller.context().getTerm())
        .withVoted(false)
        .build();
    }
    // If we've already voted for someone else then don't vote again.
    else if (controller.context().getLastVotedFor() == 0 || controller.context().getLastVotedFor() == request.candidate()) {
      if (isLogUpToDate(request.logIndex(), request.logTerm(), request)) {
        controller.context().setLastVotedFor(request.candidate());
        return VoteResponse.builder()
          .withStatus(Response.Status.OK)
          .withTerm(controller.context().getTerm())
          .withVoted(true)
          .build();
      } else {
        return VoteResponse.builder()
          .withStatus(Response.Status.OK)
          .withTerm(controller.context().getTerm())
          .withVoted(false)
          .build();
      }
    }
    // In this case, we've already voted for someone else.
    else {
      LOGGER.debug("{} - Rejected {}: already voted for {}", controller.context().getCluster().getMember().serverAddress(), request, controller.context().getLastVotedFor());
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(controller.context().getTerm())
        .withVoted(false)
        .build();
    }
  }

  /**
   * Returns a boolean value indicating whether the given candidate's log is up-to-date.
   */
  boolean isLogUpToDate(long index, long term, Request request) {
    // If the log is empty then vote for the candidate.
    if (controller.context().getLog().isEmpty()) {
      LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", controller.context().getCluster().getMember().serverAddress(), request);
      return true;
    } else {
      // Otherwise, load the last entry in the log. The last entry should be
      // at least as up to date as the candidates entry and term.
      long lastIndex = controller.context().getLog().lastIndex();
      Entry entry = controller.context().getLog().get(lastIndex);
      if (entry == null) {
        LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", controller.context().getCluster().getMember().serverAddress(), request);
        return true;
      }

      try {
        if (index != 0 && index >= lastIndex) {
          if (term >= entry.getTerm()) {
            LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", controller.context().getCluster().getMember().serverAddress(), request);
            return true;
          } else {
            LOGGER.debug("{} - Rejected {}: candidate's last log term ({}) is in conflict with local log ({})", controller.context().getCluster().getMember().serverAddress(), request, term, entry.getTerm());
            return false;
          }
        } else {
          LOGGER.debug("{} - Rejected {}: candidate's last log entry ({}) is at a lower index than the local log ({})", controller.context().getCluster().getMember().serverAddress(), request, index, lastIndex);
          return false;
        }
      } finally {
        entry.close();
      }
    }
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    controller.context().checkThread();
    logRequest(request);

    // If the query was submitted with RYW or monotonic read consistency, attempt to apply the query to the local state machine.
    if (request.query().consistency() == Query.ConsistencyLevel.CAUSAL
      || request.query().consistency() == Query.ConsistencyLevel.SEQUENTIAL) {

      // If the commit index is not in the log then we've fallen too far behind the leader to perform a local query.
      // Forward the request to the leader.
      if (controller.context().getLog().lastIndex() < controller.context().getCommitIndex()) {
        LOGGER.debug("{} - State appears to be out of sync, forwarding query to leader");
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
    if (controller.context().getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    }

    LOGGER.debug("{} - Forwarded {}", controller.context().getCluster().getMember().serverAddress(), request);
    return this.<QueryRequest, QueryResponse>forward(request).thenApply(this::logResponse);
  }

  /**
   * Performs a local query.
   */
  private CompletableFuture<QueryResponse> queryLocal(QueryRequest request) {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();

    QueryEntry entry = controller.context().getLog().create(QueryEntry.class)
      .setIndex(controller.context().getCommitIndex())
      .setTerm(controller.context().getTerm())
      .setTimestamp(System.currentTimeMillis())
      .setSession(request.session())
      .setSequence(request.sequence())
      .setVersion(request.version())
      .setQuery(request.query());

    // For CAUSAL queries, the state machine version is the last index applied to the state machine. For other consistency
    // levels, the state machine may actually wait until those queries are applied to the state machine, so the last applied
    // index is not necessarily the index at which the query will be applied, but it will be applied after its sequence.
    final long version;
    if (request.query().consistency() == Query.ConsistencyLevel.CAUSAL) {
      version = controller.context().getStateMachine().getLastApplied();
    } else {
      version = Math.max(request.sequence(), controller.context().getStateMachine().getLastApplied());
    }

    controller.context().getStateMachine().apply(entry).whenCompleteAsync((result, error) -> {
      if (isOpen()) {
        if (error == null) {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.OK)
            .withVersion(version)
            .withResult(result)
            .build()));
        } else if (error instanceof RaftException) {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withVersion(version)
            .withError(((RaftException) error).getType())
            .build()));
        } else {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withVersion(version)
            .withError(RaftError.Type.INTERNAL_ERROR)
            .build()));
        }
      }
      entry.release();
    }, controller.context().getThreadContext().executor());
    return future;
  }

}
