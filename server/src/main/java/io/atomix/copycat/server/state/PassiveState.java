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

import io.atomix.catalyst.transport.Connection;
import io.atomix.copycat.client.error.RaftError;
import io.atomix.copycat.client.request.*;
import io.atomix.copycat.client.response.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.request.*;
import io.atomix.copycat.server.response.*;
import io.atomix.copycat.server.storage.entry.ConfigurationEntry;
import io.atomix.copycat.server.storage.entry.ConnectEntry;
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Passive state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class PassiveState extends AbstractState {
  private final Queue<AtomicInteger> counterPool = new ArrayDeque<>();

  public PassiveState(ServerState context) {
    super(context);
  }

  @Override
  public CopycatServer.State type() {
    return CopycatServer.State.PASSIVE;
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
   * Starts the append process.
   */
  protected AppendResponse handleAppend(AppendRequest request) {
    // If the request term is less than the current term then immediately
    // reply false and return our current term. The leader will receive
    // the updated term and step down.
    if (request.term() < context.getTerm()) {
      LOGGER.warn("{} - Rejected {}: request term is less than the current term ({})", context.getCluster().getMember().serverAddress(), request, context.getTerm());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    } else if (request.logIndex() != 0 && request.logTerm() != 0) {
      return doCheckPreviousEntry(request);
    } else {
      return doAppendEntries(request);
    }
  }

  /**
   * Checks the previous log entry for consistency.
   */
  protected AppendResponse doCheckPreviousEntry(AppendRequest request) {
    if (request.logIndex() != 0 && context.getLog().isEmpty()) {
      LOGGER.warn("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getCluster().getMember().serverAddress(), request, request.logIndex(), context.getLog().lastIndex());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    } else if (request.logIndex() != 0 && context.getLog().lastIndex() != 0 && request.logIndex() > context.getLog().lastIndex()) {
      LOGGER.warn("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", context.getCluster().getMember().serverAddress(), request, request.logIndex(), context.getLog().lastIndex());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build();
    }

    // If the previous entry term doesn't match the local previous term then reject the request.
    try (Entry entry = context.getLog().get(request.logIndex())) {
      if (entry == null || entry.getTerm() != request.logTerm()) {
        LOGGER.warn("{} - Rejected {}: Request log term does not match local log term {} for the same entry", context.getCluster().getMember().serverAddress(), request, entry != null ? entry.getTerm() : "unknown");
        return AppendResponse.builder()
          .withStatus(Response.Status.OK)
          .withTerm(context.getTerm())
          .withSucceeded(false)
          .withLogIndex(request.logIndex() <= context.getLog().lastIndex() ? request.logIndex() - 1 : context.getLog().lastIndex())
          .build();
      } else {
        return doAppendEntries(request);
      }
    }
  }

  /**
   * Appends entries to the local log.
   */
  protected AppendResponse doAppendEntries(AppendRequest request) {
    // If the log contains entries after the request's previous log index
    // then remove those entries to be replaced by the request entries.
    if (!request.entries().isEmpty()) {

      // Iterate through request entries and append them to the log.
      for (Entry entry : request.entries()) {
        // If the entry index is greater than the last log index, skip missing entries.
        if (context.getLog().lastIndex() < entry.getIndex()) {
          context.getLog().skip(entry.getIndex() - context.getLog().lastIndex() - 1).append(entry);
          LOGGER.debug("{} - Appended {} to log at index {}", context.getCluster().getMember().serverAddress(), entry, entry.getIndex());
        } else {
          // Compare the term of the received entry with the matching entry in the log.
          try (Entry match = context.getLog().get(entry.getIndex())) {
            if (match != null) {
              if (entry.getTerm() != match.getTerm()) {
                // We found an invalid entry in the log. Remove the invalid entry and append the new entry.
                // If appending to the log fails, apply commits and reply false to the append request.
                LOGGER.warn("{} - Appended entry term does not match local log, removing incorrect entries", context.getCluster().getMember().serverAddress());
                context.getLog().truncate(entry.getIndex() - 1).append(entry);
                LOGGER.debug("{} - Appended {} to log at index {}", context.getCluster().getMember().serverAddress(), entry, entry.getIndex());
              }
            } else {
              context.getLog().truncate(entry.getIndex() - 1).append(entry);
              LOGGER.debug("{} - Appended {} to log at index {}", context.getCluster().getMember().serverAddress(), entry, entry.getIndex());
            }
          }
        }

        // If the entry is a configuration entry then immediately configure the cluster.
        if (entry instanceof ConfigurationEntry) {
          ConfigurationEntry configurationEntry = (ConfigurationEntry) entry;
          if (context.getCluster().getMember().type() == RaftMemberType.PASSIVE) {
            context.getCluster().configure(entry.getIndex(), configurationEntry.getMembers());
            if (context.getCluster().getMember().type() == RaftMemberType.ACTIVE) {
              transition(CopycatServer.State.FOLLOWER);
            }
          } else {
            context.getCluster().configure(entry.getIndex(), configurationEntry.getMembers());
            if (context.getCluster().getMember().type() == RaftMemberType.PASSIVE) {
              transition(CopycatServer.State.PASSIVE);
            }
          }
        } else if (entry instanceof ConnectEntry) {
          ConnectEntry connectEntry = (ConnectEntry) entry;
          context.getStateMachine().executor().context().sessions().registerAddress(connectEntry.getSession(), connectEntry.getAddress());
        }
      }
    }

    // If we've made it this far, apply commits and send a successful response.
    long commitIndex = request.commitIndex();
    context.getThreadContext().execute(() -> applyCommits(commitIndex)).thenRun(() -> {
      context.getLog().compactor().minorIndex(context.getLastCompleted());
    });

    return AppendResponse.builder()
      .withStatus(Response.Status.OK)
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .withLogIndex(context.getLog().lastIndex())
      .build();
  }

  /**
   * Applies commits to the local state machine.
   */
  protected CompletableFuture<Void> applyCommits(long commitIndex) {
    // Set the commit index, ensuring that the index cannot be decreased.
    context.setCommitIndex(Math.max(context.getCommitIndex(), commitIndex));

    // The entries to be applied to the state machine are the difference between min(lastIndex, commitIndex) and lastApplied.
    long lastIndex = context.getLog().lastIndex();
    long lastApplied = context.getLastApplied();

    long effectiveIndex = Math.min(lastIndex, context.getCommitIndex());

    // If the effective commit index is greater than the last index applied to the state machine then apply remaining entries.
    if (effectiveIndex > lastApplied) {
      long entriesToApply = effectiveIndex - lastApplied;
      LOGGER.debug("{} - Applying {} commits", context.getCluster().getMember().serverAddress(), entriesToApply);

      CompletableFuture<Void> future = new CompletableFuture<>();

      // Rather than composing all futures into a single future, use a counter to count completions in order to preserve memory.
      AtomicInteger counter = getCounter();

      for (long i = lastApplied + 1; i <= effectiveIndex; i++) {
        Entry entry = context.getLog().get(i);
        if (entry != null) {
          applyEntry(entry).whenComplete((result, error) -> {
            if (isOpen() && error != null) {
              LOGGER.info("{} - An application error occurred: {}", context.getCluster().getMember().serverAddress(), error.getMessage());
            }

            if (counter.incrementAndGet() == entriesToApply) {
              future.complete(null);
              recycleCounter(counter);
            }
            entry.release();
          });
        }
      }
      return future;
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Gets a counter from the counter pool.
   */
  private AtomicInteger getCounter() {
    AtomicInteger counter = counterPool.poll();
    if (counter == null)
      counter = new AtomicInteger();
    counter.set(0);
    return counter;
  }

  /**
   * Adds a used counter to the counter pool.
   */
  private void recycleCounter(AtomicInteger counter) {
    counterPool.add(counter);
  }

  /**
   * Applies an entry to the state machine.
   */
  protected CompletableFuture<?> applyEntry(Entry entry) {
    LOGGER.debug("{} - Applying {}", context.getCluster().getMember().serverAddress(), entry);
    return context.getStateMachine().apply(entry);
  }

  @Override
  protected CompletableFuture<PollResponse> poll(PollRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<VoteResponse> vote(VoteRequest request) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  /**
   * Forwards the given request to the leader if possible.
   */
  protected <T extends Request<T>, U extends Response<U>> CompletableFuture<U> forward(T request) {
    CompletableFuture<U> future = new CompletableFuture<>();
    context.getConnections().getConnection(context.getLeader()).whenComplete((connection, connectError) -> {
      if (connectError == null) {
        connection.<T, U>send(request).whenComplete((response, responseError) -> {
          if (responseError == null) {
            future.complete(response);
          } else {
            future.completeExceptionally(responseError);
          }
        });
      } else {
        future.completeExceptionally(connectError);
      }
    });
    return future;
  }

  @Override
  protected CompletableFuture<CommandResponse> command(CommandRequest request) {
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
  }

  @Override
  protected CompletableFuture<QueryResponse> query(QueryRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<QueryRequest, QueryResponse>forward(request).thenApply(this::logResponse);
    }
  }

  @Override
  protected CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(RegisterResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<ConnectResponse> connect(ConnectRequest request, Connection connection) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(ConnectResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<AcceptResponse> accept(AcceptRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(AcceptResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(KeepAliveResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withLeader(context.getLeader())
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(UnregisterResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<PublishResponse> publish(PublishRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(PublishResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<JoinResponse> join(JoinRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<JoinRequest, JoinResponse>forward(request).thenApply(this::logResponse);
    }
  }

  @Override
  protected CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<LeaveRequest, LeaveResponse>forward(request).thenApply(this::logResponse);
    }
  }

}
