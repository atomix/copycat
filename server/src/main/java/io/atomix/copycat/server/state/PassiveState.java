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
import io.atomix.copycat.server.cluster.MemberType;
import io.atomix.copycat.server.controller.ServerStateController;
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
class PassiveState extends RaftState {
  private final Queue<AtomicInteger> counterPool = new ArrayDeque<>();

  public PassiveState(ServerStateController controller) {
    super(controller);
  }

  @Override
  public Type type() {
    return RaftStateType.PASSIVE;
  }

  @Override
  public CompletableFuture<AppendResponse> append(final AppendRequest request) {
    controller.context().checkThread();

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > controller.context().getTerm() || (request.term() == controller.context().getTerm() && controller.context().getLeader() == null)) {
      controller.context().setTerm(request.term());
      controller.context().setLeader(request.leader());
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
    if (request.term() < controller.context().getTerm()) {
      LOGGER.warn("{} - Rejected {}: request term is less than the current term ({})", controller.context().getCluster().getMember().serverAddress(), request, controller.context().getTerm());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(controller.context().getTerm())
        .withSucceeded(false)
        .withLogIndex(controller.context().getLog().lastIndex())
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
    if (request.logIndex() != 0 && controller.context().getLog().isEmpty()) {
      LOGGER.warn("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", controller.context().getCluster().getMember().serverAddress(), request, request.logIndex(), controller.context().getLog().lastIndex());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(controller.context().getTerm())
        .withSucceeded(false)
        .withLogIndex(controller.context().getLog().lastIndex())
        .build();
    } else if (request.logIndex() != 0 && controller.context().getLog().lastIndex() != 0 && request.logIndex() > controller.context().getLog().lastIndex()) {
      LOGGER.warn("{} - Rejected {}: Previous index ({}) is greater than the local log's last index ({})", controller.context().getCluster().getMember().serverAddress(), request, request.logIndex(), controller.context().getLog().lastIndex());
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(controller.context().getTerm())
        .withSucceeded(false)
        .withLogIndex(controller.context().getLog().lastIndex())
        .build();
    }

    // If the previous entry term doesn't match the local previous term then reject the request.
    try (Entry entry = controller.context().getLog().get(request.logIndex())) {
      if (entry == null || entry.getTerm() != request.logTerm()) {
        LOGGER.warn("{} - Rejected {}: Request log term does not match local log term {} for the same entry", controller.context().getCluster().getMember().serverAddress(), request, entry != null ? entry.getTerm() : "unknown");
        return AppendResponse.builder()
          .withStatus(Response.Status.OK)
          .withTerm(controller.context().getTerm())
          .withSucceeded(false)
          .withLogIndex(request.logIndex() <= controller.context().getLog().lastIndex() ? request.logIndex() - 1 : controller.context().getLog().lastIndex())
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
        if (controller.context().getLog().lastIndex() < entry.getIndex()) {
          controller.context().getLog().skip(entry.getIndex() - controller.context().getLog().lastIndex() - 1).append(entry);
          LOGGER.debug("{} - Appended {} to log at index {}", controller.context().getCluster().getMember().serverAddress(), entry, entry.getIndex());
        } else {
          // Compare the term of the received entry with the matching entry in the log.
          try (Entry match = controller.context().getLog().get(entry.getIndex())) {
            if (match != null) {
              if (entry.getTerm() != match.getTerm()) {
                // We found an invalid entry in the log. Remove the invalid entry and append the new entry.
                // If appending to the log fails, apply commits and reply false to the append request.
                LOGGER.warn("{} - Appended entry term does not match local log, removing incorrect entries", controller.context().getCluster().getMember().serverAddress());
                controller.context().getLog().truncate(entry.getIndex() - 1).append(entry);
                LOGGER.debug("{} - Appended {} to log at index {}", controller.context().getCluster().getMember().serverAddress(), entry, entry.getIndex());
              }
            } else {
              controller.context().getLog().truncate(entry.getIndex() - 1).append(entry);
              LOGGER.debug("{} - Appended {} to log at index {}", controller.context().getCluster().getMember().serverAddress(), entry, entry.getIndex());
            }
          }
        }

        // If the entry is a configuration entry then immediately configure the cluster.
        if (entry instanceof ConfigurationEntry) {
          ConfigurationEntry configurationEntry = (ConfigurationEntry) entry;
          controller.context().getCluster().configure(entry.getIndex(), configurationEntry.getMembers());
          controller.context().configure(controller.context().getCluster().getMember().type());
        } else if (entry instanceof ConnectEntry) {
          ConnectEntry connectEntry = (ConnectEntry) entry;
          controller.context().getStateMachine().executor().context().sessions().registerAddress(connectEntry.getSession(), connectEntry.getAddress());
        }
      }
    }

    // If we've made it this far, apply commits and send a successful response.
    long commitIndex = request.commitIndex();
    controller.context().getThreadContext().execute(() -> applyCommits(commitIndex)).thenRun(() -> {
      controller.context().getLog().compactor().minorIndex(controller.context().getLastCompleted());
    });

    return AppendResponse.builder()
      .withStatus(Response.Status.OK)
      .withTerm(controller.context().getTerm())
      .withSucceeded(true)
      .withLogIndex(controller.context().getLog().lastIndex())
      .build();
  }

  /**
   * Applies commits to the local state machine.
   */
  protected CompletableFuture<Void> applyCommits(long commitIndex) {
    // Set the commit index, ensuring that the index cannot be decreased.
    controller.context().setCommitIndex(Math.max(controller.context().getCommitIndex(), commitIndex));

    // The entries to be applied to the state machine are the difference between min(lastIndex, commitIndex) and lastApplied.
    long lastIndex = controller.context().getLog().lastIndex();
    long lastApplied = controller.context().getLastApplied();

    long effectiveIndex = Math.min(lastIndex, controller.context().getCommitIndex());

    // If the effective commit index is greater than the last index applied to the state machine then apply remaining entries.
    if (effectiveIndex > lastApplied) {
      long entriesToApply = effectiveIndex - lastApplied;
      LOGGER.debug("{} - Applying {} commits", controller.context().getCluster().getMember().serverAddress(), entriesToApply);

      CompletableFuture<Void> future = new CompletableFuture<>();

      // Rather than composing all futures into a single future, use a counter to count completions in order to preserve memory.
      AtomicInteger counter = getCounter();

      for (long i = lastApplied + 1; i <= effectiveIndex; i++) {
        Entry entry = controller.context().getLog().get(i);
        if (entry != null) {
          applyEntry(entry).whenComplete((result, error) -> {
            if (isOpen() && error != null) {
              LOGGER.info("{} - An application error occurred: {}", controller.context().getCluster().getMember().serverAddress(), error.getMessage());
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
    LOGGER.debug("{} - Applying {}", controller.context().getCluster().getMember().serverAddress(), entry);
    return controller.context().getStateMachine().apply(entry);
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    controller.context().checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> vote(VoteRequest request) {
    controller.context().checkThread();
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
    controller.context().getConnections().getConnection(controller.context().getLeader().serverAddress()).whenComplete((connection, connectError) -> {
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
  public CompletableFuture<CommandResponse> command(CommandRequest request) {
    controller.context().checkThread();
    logRequest(request);
    if (controller.context().getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<CommandRequest, CommandResponse>forward(request).thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    controller.context().checkThread();
    logRequest(request);
    if (controller.context().getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<QueryRequest, QueryResponse>forward(request).thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    controller.context().checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(RegisterResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<ConnectResponse> connect(ConnectRequest request, Connection connection) {
    controller.context().checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(ConnectResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<AcceptResponse> accept(AcceptRequest request) {
    controller.context().checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(AcceptResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    controller.context().checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(KeepAliveResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withLeader(controller.context().getLeader().serverAddress())
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request) {
    controller.context().checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(UnregisterResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<PublishResponse> publish(PublishRequest request) {
    controller.context().checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(PublishResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(ConfigureRequest request) {
    controller.context().checkThread();
    logRequest(request);

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > controller.context().getTerm() || (request.term() == controller.context().getTerm() && controller.context().getLeader() == null)) {
      controller.context().setTerm(request.term());
      controller.context().setLeader(request.leader());
    }

    // Store the previous member type for comparison to determine whether this node should transition.
    MemberType previousType = controller.context().getCluster().getMember().type();

    // Configure the cluster membership.
    controller.context().getCluster().configure(request.version(), request.members());

    // If the local member type changed, transition the state as appropriate.
    // ACTIVE servers are initialized to the FOLLOWER state but may transition to CANDIDATE or LEADER.
    // PASSIVE servers are transitioned to the PASSIVE state.
    MemberType type = controller.context().getCluster().getMember().type();
    if (previousType != type) {
      controller.context().configure(type);
    }

    return CompletableFuture.completedFuture(logResponse(ConfigureResponse.builder()
      .withStatus(Response.Status.OK)
      .build()));
  }

  @Override
  public CompletableFuture<JoinResponse> join(JoinRequest request) {
    controller.context().checkThread();
    logRequest(request);

    if (controller.context().getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<JoinRequest, JoinResponse>forward(request).thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    controller.context().checkThread();
    logRequest(request);

    if (controller.context().getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<LeaveRequest, LeaveResponse>forward(request).thenApply(this::logResponse);
    }
  }

}
