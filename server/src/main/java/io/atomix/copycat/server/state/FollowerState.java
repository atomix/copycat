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
import io.atomix.catalyst.util.concurrent.Scheduled;
import io.atomix.copycat.client.error.RaftError;
import io.atomix.copycat.client.request.*;
import io.atomix.copycat.client.response.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.request.AcceptRequest;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.request.PollRequest;
import io.atomix.copycat.server.request.VoteRequest;
import io.atomix.copycat.server.response.AcceptResponse;
import io.atomix.copycat.server.response.AppendResponse;
import io.atomix.copycat.server.response.PollResponse;
import io.atomix.copycat.server.response.VoteResponse;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.server.util.Quorum;

import java.time.Duration;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Follower state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class FollowerState extends ActiveState {
  private final Random random = new Random();
  private Scheduled heartbeatTimer;

  public FollowerState(ServerState context) {
    super(context);
  }

  @Override
  public CopycatServer.State type() {
    return CopycatServer.State.FOLLOWER;
  }

  @Override
  public synchronized CompletableFuture<AbstractState> open() {
    return super.open().thenRun(this::startHeartbeatTimeout).thenApply(v -> this);
  }

  @Override
  protected CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    try {
      context.checkThread();
      logRequest(request);

      if (context.getLeader() == null) {
        return CompletableFuture.completedFuture(logResponse(RegisterResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
      } else {
        return this.<RegisterRequest, RegisterResponse>forward(request).thenApply(this::logResponse);
      }
    } finally {
      request.release();
    }
  }

  @Override
  protected CompletableFuture<ConnectResponse> connect(ConnectRequest request, Connection connection) {
    try {
      context.checkThread();
      logRequest(request);

      if (context.getLeader() == null) {
        return CompletableFuture.completedFuture(logResponse(ConnectResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
      } else {
        // Immediately register the session connection and send an accept request to the leader.
        context.getStateMachine().executor().context().sessions().registerConnection(request.session(), connection);

        AcceptRequest acceptRequest = AcceptRequest.builder()
          .withSession(request.session())
          .withAddress(context.getAddress())
          .build();
        return this.<AcceptRequest, AcceptResponse>forward(acceptRequest)
          .thenApply(acceptResponse -> ConnectResponse.builder().withStatus(Response.Status.OK).build())
          .thenApply(this::logResponse);
      }
    } finally {
      request.release();
    }
  }

  @Override
  protected CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    try {
      context.checkThread();
      logRequest(request);

      if (context.getLeader() == null) {
        return CompletableFuture.completedFuture(logResponse(KeepAliveResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
      } else {
        return this.<KeepAliveRequest, KeepAliveResponse>forward(request).thenApply(this::logResponse);
      }
    } finally {
      request.release();
    }
  }

  @Override
  protected CompletableFuture<PublishResponse> publish(PublishRequest request) {
    try {
      context.checkThread();
      logRequest(request);

      ServerSession session = context.getStateMachine().executor().context().sessions().getSession(request.session());
      if (session == null || session.getConnection() == null) {
        return CompletableFuture.completedFuture(logResponse(PublishResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
          .build()));
      } else {
        return session.getConnection().<PublishRequest, PublishResponse>send(request.acquire());
      }
    } finally {
      request.release();
    }
  }

  @Override
  protected CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request) {
    try {
      context.checkThread();
      logRequest(request);

      if (context.getLeader() == null) {
        return CompletableFuture.completedFuture(logResponse(UnregisterResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(RaftError.Type.NO_LEADER_ERROR)
          .build()));
      } else {
        return this.<UnregisterRequest, UnregisterResponse>forward(request).thenApply(this::logResponse);
      }
    } finally {
      request.release();
    }
  }

  /**
   * Starts the heartbeat timer.
   */
  private void startHeartbeatTimeout() {
    LOGGER.debug("{} - Starting heartbeat timer", context.getAddress());
    resetHeartbeatTimeout();
  }

  /**
   * Resets the heartbeat timer.
   */
  private void resetHeartbeatTimeout() {
    context.checkThread();
    if (isClosed())
      return;

    // If a timer is already set, cancel the timer.
    if (heartbeatTimer != null) {
      LOGGER.debug("{} - Reset heartbeat timeout", context.getAddress());
      heartbeatTimer.cancel();
    }

    // Set the election timeout in a semi-random fashion with the random range
    // being election timeout and 2 * election timeout.
    Duration delay = context.getElectionTimeout().plus(Duration.ofMillis(random.nextInt((int) context.getElectionTimeout().toMillis())));
    heartbeatTimer = context.getThreadContext().schedule(delay, () -> {
      heartbeatTimer = null;
      if (isOpen()) {
        context.setLeader(0);
        if (context.getLastVotedFor() == 0) {
          LOGGER.debug("{} - Heartbeat timed out in {} milliseconds", context.getAddress(), delay);
          sendPollRequests();
        } else {
          // If the node voted for a candidate then reset the election timer.
          resetHeartbeatTimeout();
        }
      }
    });
  }

  /**
   * Polls all members of the cluster to determine whether this member should transition to the CANDIDATE state.
   */
  private void sendPollRequests() {
    // Set a new timer within which other nodes must respond in order for this node to transition to candidate.
    heartbeatTimer = context.getThreadContext().schedule(context.getElectionTimeout(), () -> {
      LOGGER.debug("{} - Failed to poll a majority of the cluster in {} milliseconds", context.getAddress(), context.getElectionTimeout());
      resetHeartbeatTimeout();
    });

    // Create a quorum that will track the number of nodes that have responded to the poll request.
    final AtomicBoolean complete = new AtomicBoolean();
    final Set<MemberState> votingMembers = new HashSet<>(context.getCluster().getActiveMembers());

    // If there are no other members in the cluster, immediately transition to leader.
    if (votingMembers.isEmpty()) {
      LOGGER.debug("{} - Single member cluster. Transitioning directly to leader.", context.getAddress());
      transition(CopycatServer.State.LEADER);
      return;
    }

    final Quorum quorum = new Quorum(context.getCluster().getQuorum(), (elected) -> {
      // If a majority of the cluster indicated they would vote for us then transition to candidate.
      complete.set(true);
      if (elected) {
        transition(CopycatServer.State.CANDIDATE);
      } else {
        resetHeartbeatTimeout();
      }
    });

    // First, load the last log entry to get its term. We load the entry
    // by its index since the index is required by the protocol.
    long lastIndex = context.getLog().lastIndex();
    Entry lastEntry = lastIndex > 0 ? context.getLog().get(lastIndex) : null;

    final long lastTerm;
    if (lastEntry != null) {
      lastTerm = lastEntry.getTerm();
      lastEntry.close();
    } else {
      lastTerm = 0;
    }

    LOGGER.debug("{} - Polling members {}", context.getAddress(), votingMembers);

    // Once we got the last log term, iterate through each current member
    // of the cluster and vote each member for a vote.
    for (MemberState member : votingMembers) {
      LOGGER.debug("{} - Polling {} for next term {}", context.getAddress(), member, context.getTerm() + 1);
      PollRequest request = PollRequest.builder()
        .withTerm(context.getTerm())
        .withCandidate(context.getAddress().hashCode())
        .withLogIndex(lastIndex)
        .withLogTerm(lastTerm)
        .build();
      context.getConnections().getConnection(member.getAddress()).thenAccept(connection -> {
        connection.<PollRequest, PollResponse>send(request).whenCompleteAsync((response, error) -> {
          context.checkThread();
          if (isOpen() && !complete.get()) {
            if (error != null) {
              LOGGER.warn("{} - {}", context.getAddress(), error.getMessage());
              quorum.fail();
            } else {
              if (response.term() > context.getTerm()) {
                context.setTerm(response.term());
              }

              if (!response.accepted()) {
                LOGGER.debug("{} - Received rejected poll from {}", context.getAddress(), member);
                quorum.fail();
              } else if (response.term() != context.getTerm()) {
                LOGGER.debug("{} - Received accepted poll for a different term from {}", context.getAddress(), member);
                quorum.fail();
              } else {
                LOGGER.debug("{} - Received accepted poll from {}", context.getAddress(), member);
                quorum.succeed();
              }
              response.release();
            }
          } else if (response != null) {
            response.release();
          }
        }, context.getThreadContext().executor());
      });
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    resetHeartbeatTimeout();
    CompletableFuture<AppendResponse> response = super.append(request);
    resetHeartbeatTimeout();
    return response;
  }

  @Override
  protected VoteResponse handleVote(VoteRequest request) {
    // Reset the heartbeat timeout if we voted for another candidate.
    VoteResponse response = super.handleVote(request);
    if (response.voted()) {
      resetHeartbeatTimeout();
    }
    return response;
  }

  /**
   * Cancels the heartbeat timeout.
   */
  private void cancelHeartbeatTimeout() {
    if (heartbeatTimer != null) {
      LOGGER.debug("{} - Cancelling heartbeat timer", context.getAddress());
      heartbeatTimer.cancel();
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelHeartbeatTimeout);
  }

}
