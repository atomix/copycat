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

import io.atomix.catalyst.util.concurrent.Scheduled;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.cluster.MemberState;
import io.atomix.copycat.server.controller.ServerStateController;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.request.VoteRequest;
import io.atomix.copycat.server.response.AppendResponse;
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
 * Candidate state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class CandidateState extends ActiveState {
  private final Random random = new Random();
  private Quorum quorum;
  private Scheduled currentTimer;

  public CandidateState(ServerStateController controller) {
    super(controller);
  }

  @Override
  public Type type() {
    return RaftStateType.CANDIDATE;
  }

  @Override
  public synchronized CompletableFuture<ServerState> open() {
    return super.open().thenRun(this::startElection).thenApply(v -> this);
  }

  /**
   * Starts the election.
   */
  void startElection() {
    LOGGER.info("{} - Starting election", controller.context().getCluster().getMember().serverAddress());
    sendVoteRequests();
  }

  /**
   * Resets the election timer.
   */
  private void sendVoteRequests() {
    controller.context().checkThread();

    // Because of asynchronous execution, the candidate state could have already been closed. In that case,
    // simply skip the election.
    if (isClosed()) return;

    // Cancel the current timer task and purge the election timer of cancelled tasks.
    if (currentTimer != null) {
      currentTimer.cancel();
    }

    // When the election timer is reset, increment the current term and
    // restart the election.
    controller.context().setTerm(controller.context().getTerm() + 1).setLastVotedFor(controller.context().getCluster().getMember().serverAddress().hashCode());

    Duration delay = controller.context().getElectionTimeout().plus(Duration.ofMillis(random.nextInt((int) controller.context().getElectionTimeout().toMillis())));
    currentTimer = controller.context().getThreadContext().schedule(delay, () -> {
      // When the election times out, clear the previous majority vote
      // check and restart the election.
      LOGGER.debug("{} - Election timed out", controller.context().getCluster().getMember().serverAddress());
      if (quorum != null) {
        quorum.cancel();
        quorum = null;
      }
      sendVoteRequests();
      LOGGER.debug("{} - Restarted election", controller.context().getCluster().getMember().serverAddress());
    });

    final AtomicBoolean complete = new AtomicBoolean();
    final Set<MemberState> votingMembers = new HashSet<>(controller.context().getCluster().getVotingMemberStates());

    // If there are no other members in the cluster, immediately transition to leader.
    if (votingMembers.isEmpty()) {
      LOGGER.debug("{} - Single member cluster. Transitioning directly to leader.", controller.context().getCluster().getMember().serverAddress());
      controller.transition(RaftStateType.LEADER);
      return;
    }

    // Send vote requests to all nodes. The vote request that is sent
    // to this node will be automatically successful.
    // First check if the quorum is null. If the quorum isn't null then that
    // indicates that another vote is already going on.
    final Quorum quorum = new Quorum(controller.context().getCluster().getQuorum(), (elected) -> {
      complete.set(true);
      if (elected) {
        controller.transition(RaftStateType.LEADER);
      } else {
        controller.transition(RaftStateType.FOLLOWER);
      }
    });

    // First, load the last log entry to get its term. We load the entry
    // by its index since the index is required by the protocol.
    long lastIndex = controller.context().getLog().lastIndex();
    Entry lastEntry = lastIndex != 0 ? controller.context().getLog().get(lastIndex) : null;

    final long lastTerm;
    if (lastEntry != null) {
      lastTerm = lastEntry.getTerm();
      lastEntry.close();
    } else {
      lastTerm = 0;
    }

    LOGGER.info("{} - Requesting votes from {}", controller.context().getCluster().getMember().serverAddress(), votingMembers);

    // Once we got the last log term, iterate through each current member
    // of the cluster and vote each member for a vote.
    for (MemberState member : votingMembers) {
      LOGGER.debug("{} - Requesting vote from {} for term {}", controller.context().getCluster().getMember().serverAddress(), member, controller.context().getTerm());
      VoteRequest request = VoteRequest.builder()
        .withTerm(controller.context().getTerm())
        .withCandidate(controller.context().getCluster().getMember().serverAddress().hashCode())
        .withLogIndex(lastIndex)
        .withLogTerm(lastTerm)
        .build();

      controller.context().getConnections().getConnection(member.getMember().serverAddress()).thenAccept(connection -> {
        connection.<VoteRequest, VoteResponse>send(request).whenCompleteAsync((response, error) -> {
          controller.context().checkThread();
          if (isOpen() && !complete.get()) {
            if (error != null) {
              LOGGER.warn(error.getMessage());
              quorum.fail();
            } else {
              if (response.term() > controller.context().getTerm()) {
                LOGGER.debug("{} - Received greater term from {}", controller.context().getCluster().getMember().serverAddress(), member);
                controller.context().setTerm(response.term());
                complete.set(true);
                controller.transition(RaftStateType.FOLLOWER);
              } else if (!response.voted()) {
                LOGGER.debug("{} - Received rejected vote from {}", controller.context().getCluster().getMember().serverAddress(), member);
                quorum.fail();
              } else if (response.term() != controller.context().getTerm()) {
                LOGGER.debug("{} - Received successful vote for a different term from {}", controller.context().getCluster().getMember().serverAddress(), member);
                quorum.fail();
              } else {
                LOGGER.debug("{} - Received successful vote from {}", controller.context().getCluster().getMember().serverAddress(), member);
                quorum.succeed();
              }
            }
          }
        }, controller.context().getThreadContext().executor());
      });
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    controller.context().checkThread();

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as a candidate.
    if (request.term() >= controller.context().getTerm()) {
      controller.context().setTerm(request.term());
      controller.transition(RaftStateType.FOLLOWER);
    }
    return super.append(request);
  }

  @Override
  public CompletableFuture<VoteResponse> vote(VoteRequest request) {
    controller.context().checkThread();

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as a candidate.
    if (request.term() > controller.context().getTerm()) {
      controller.context().setTerm(request.term());
      controller.transition(RaftStateType.FOLLOWER);
      return super.vote(request);
    }

    // If the vote request is not for this candidate then reject the vote.
    if (request.candidate() == controller.context().getCluster().getMember().serverAddress().hashCode()) {
      return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(controller.context().getTerm())
        .withVoted(true)
        .build()));
    } else {
      return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(controller.context().getTerm())
        .withVoted(false)
        .build()));
    }
  }

  /**
   * Cancels the election.
   */
  private void cancelElection() {
    controller.context().checkThread();
    if (currentTimer != null) {
      LOGGER.debug("{} - Cancelling election", controller.context().getCluster().getMember().serverAddress());
      currentTimer.cancel();
    }
    if (quorum != null) {
      quorum.cancel();
      quorum = null;
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close().thenRun(this::cancelElection);
  }

}
