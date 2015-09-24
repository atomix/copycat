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

import io.atomix.catalog.client.Query;
import io.atomix.catalog.client.error.RaftError;
import io.atomix.catalog.client.error.RaftException;
import io.atomix.catalog.client.request.CommandRequest;
import io.atomix.catalog.client.request.QueryRequest;
import io.atomix.catalog.client.request.Request;
import io.atomix.catalog.client.response.CommandResponse;
import io.atomix.catalog.client.response.QueryResponse;
import io.atomix.catalog.client.response.Response;
import io.atomix.catalog.server.RaftServer;
import io.atomix.catalog.server.request.AppendRequest;
import io.atomix.catalog.server.request.PollRequest;
import io.atomix.catalog.server.request.VoteRequest;
import io.atomix.catalog.server.response.AppendResponse;
import io.atomix.catalog.server.response.PollResponse;
import io.atomix.catalog.server.response.VoteResponse;
import io.atomix.catalog.server.storage.entry.Entry;
import io.atomix.catalog.server.storage.entry.QueryEntry;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Abstract active state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class ActiveState extends PassiveState {

  protected ActiveState(ServerState context) {
    super(context);
  }

  @Override
  protected CompletableFuture<AppendResponse> append(final AppendRequest request) {
    try {
      context.checkThread();

      // If the request indicates a term that is greater than the current term then
      // assign that term and leader to the current context and step down as leader.
      boolean transition = false;
      if (request.term() > context.getTerm() || (request.term() == context.getTerm() && context.getLeader() == null)) {
        context.setTerm(request.term());
        context.setLeader(request.leader());
        transition = true;
      }

      CompletableFuture<AppendResponse> future = CompletableFuture.completedFuture(logResponse(handleAppend(logRequest(request))));

      // If a transition is required then transition back to the follower state.
      // If the node is already a follower then the transition will be ignored.
      if (transition) {
        transition(RaftServer.State.FOLLOWER);
      }
      return future;
    } finally {
      request.release();
    }
  }

  @Override
  protected CompletableFuture<PollResponse> poll(PollRequest request) {
    try {
      context.checkThread();
      return CompletableFuture.completedFuture(logResponse(handlePoll(logRequest(request))));
    } finally {
      request.release();
    }
  }

  /**
   * Handles a poll request.
   */
  protected PollResponse handlePoll(PollRequest request) {
    if (logUpToDate(request.logIndex(), request.logTerm(), request)) {
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
    try {
      context.checkThread();
      return CompletableFuture.completedFuture(logResponse(handleVote(logRequest(request))));
    } finally {
      request.release();
    }
  }

  /**
   * Handles a vote request.
   */
  protected VoteResponse handleVote(VoteRequest request) {
    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm()) {
      context.setTerm(request.term());
    }

    // If the request term is not as great as the current context term then don't
    // vote for the candidate. We want to vote for candidates that are at least
    // as up to date as us.
    if (request.term() < context.getTerm()) {
      LOGGER.debug("{} - Rejected {}: candidate's term is less than the current term", context.getAddress(), request);
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If the requesting candidate is not a known member of the cluster (to this
    // node) then don't vote for it. Only vote for candidates that we know about.
    else if (!context.getCluster().getActiveMembers().stream().<Integer>map(m -> m.getAddress().hashCode()).collect(Collectors.toSet()).contains(request.candidate())) {
      LOGGER.debug("{} - Rejected {}: candidate is not known to the local member", context.getAddress(), request);
      return VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build();
    }
    // If we've already voted for someone else then don't vote again.
    else if (context.getLastVotedFor() == 0 || context.getLastVotedFor() == request.candidate()) {
      if (logUpToDate(request.logIndex(), request.logTerm(), request)) {
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
    // In this case, we've already voted for someone else.
    else {
      LOGGER.debug("{} - Rejected {}: already voted for {}", context.getAddress(), request, context.getLastVotedFor());
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
  private boolean logUpToDate(long index, long term, Request request) {
    // If the log is empty then vote for the candidate.
    if (context.getLog().isEmpty()) {
      LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getAddress(), request);
      return true;
    } else {
      // Otherwise, load the last entry in the log. The last entry should be
      // at least as up to date as the candidates entry and term.
      long lastIndex = context.getLog().lastIndex();
      Entry entry = context.getLog().get(lastIndex);
      if (entry == null) {
        LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getAddress(), request);
        return true;
      }

      try {
        if (index != 0 && index >= lastIndex) {
          if (term >= entry.getTerm()) {
            LOGGER.debug("{} - Accepted {}: candidate's log is up-to-date", context.getAddress(), request);
            return true;
          } else {
            LOGGER.debug("{} - Rejected {}: candidate's last log term ({}) is in conflict with local log ({})", context.getAddress(), request, term, entry.getTerm());
            return false;
          }
        } else {
          LOGGER.debug("{} - Rejected {}: candidate's last log entry ({}) is at a lower index than the local log ({})", context.getAddress(), request, index, lastIndex);
          return false;
        }
      } finally {
        entry.close();
      }
    }
  }

  @Override
  protected CompletableFuture<CommandResponse> command(CommandRequest request) {
    try {
      context.checkThread();
      logRequest(request);

      if (context.getLeader() == null) {
        return CompletableFuture.completedFuture(logResponse(CommandResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
      } else {
        return this.<CommandRequest, CommandResponse>forward(request).thenApply(this::logResponse);
      }
    } finally {
      request.release();
    }
  }

  @Override
  protected CompletableFuture<QueryResponse> query(QueryRequest request) {
    try {
      context.checkThread();
      logRequest(request);

      // If the query was submitted with RYW or monotonic read consistency, attempt to apply the query to the local state machine.
      if (request.query().consistency() == Query.ConsistencyLevel.CAUSAL
        || request.query().consistency() == Query.ConsistencyLevel.SEQUENTIAL) {

        // If the commit index is not in the log then we've fallen too far behind the leader to perform a local query.
        // Forward the request to the leader.
        if (context.getLog().lastIndex() < context.getCommitIndex()) {
          LOGGER.debug("{} - State appears to be out of sync, forwarding query to leader");
          return queryForward(request);
        }

        return queryLocal(request);
      } else {
        return queryForward(request);
      }
    } finally {
      request.release();
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

    LOGGER.debug("{} - Forwarded {}", context.getAddress(), request);
    return this.<QueryRequest, QueryResponse>forward(request).thenApply(this::logResponse);
  }

  /**
   * Performs a local query.
   */
  private CompletableFuture<QueryResponse> queryLocal(QueryRequest request) {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();

    QueryEntry entry = context.getLog().create(QueryEntry.class)
      .setIndex(context.getCommitIndex())
      .setId(0)
      .setTerm(context.getTerm())
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
      version = context.getStateMachine().getLastApplied();
    } else {
      version = Math.max(request.sequence(), context.getStateMachine().getLastApplied());
    }

    context.getStateMachine().apply(entry).whenCompleteAsync((result, error) -> {
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
    }, context.getThreadContext().executor());
    return future;
  }

}
