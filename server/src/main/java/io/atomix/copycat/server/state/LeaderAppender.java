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
 * limitations under the License
 */
package io.atomix.copycat.server.state;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.error.InternalException;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.request.InstallRequest;
import io.atomix.copycat.server.response.AppendResponse;
import io.atomix.copycat.server.response.InstallResponse;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The leader appender is responsible for sending {@link AppendRequest}s on behalf of a leader to followers.
 * Append requests are sent by the leader only to other active members of the cluster.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class LeaderAppender extends AbstractAppender {
  private final LeaderState leader;
  private final long leaderTime;
  private final long leaderIndex;
  private long heartbeatTime;
  private int appendFailures;
  private CompletableFuture<Long> heartbeatFuture;
  private CompletableFuture<Long> nextHeartbeatFuture;
  private final Map<Long, CompletableFuture<Long>> appendFutures = new HashMap<>();
  private final Map<Long, CompletableFuture<Long>> commitFutures = new HashMap<>();

  LeaderAppender(LeaderState leader) {
    super(leader.context);
    this.leader = Assert.notNull(leader, "leader");
    this.leaderTime = System.currentTimeMillis();
    this.leaderIndex = context.getLog().nextIndex();
    this.heartbeatTime = leaderTime;
  }

  /**
   * Returns the current commit time.
   *
   * @return The current commit time.
   */
  public long time() {
    return heartbeatTime;
  }

  /**
   * Returns the leader index.
   *
   * @return The leader index.
   */
  public long index() {
    return leaderIndex;
  }

  /**
   * Returns the current quorum index.
   *
   * @return The current quorum index.
   */
  private int quorumIndex() {
    return context.getClusterState().getQuorum() - 2;
  }

  /**
   * Triggers a heartbeat to a majority of the cluster.
   * <p>
   * For followers to which no AppendRequest is currently being sent, a new empty AppendRequest will be
   * created and sent. For followers to which an AppendRequest is already being sent, the appendEntries()
   * call will piggyback on the *next* AppendRequest. Thus, multiple calls to this method will only ever
   * result in a single AppendRequest to each follower at any given time, and the returned future will be
   * shared by all concurrent calls.
   *
   * @return A completable future to be completed the next time a heartbeat is received by a majority of the cluster.
   */
  public CompletableFuture<Long> appendEntries() {
    // If there are no other active members in the cluster, simply complete the append operation.
    if (context.getClusterState().getRemoteMemberStates().isEmpty())
      return CompletableFuture.completedFuture(null);

    // If no commit future already exists, that indicates there's no heartbeat currently under way.
    // Create a new commit future and commit to all members in the cluster.
    if (heartbeatFuture == null) {
      CompletableFuture<Long> newHeartbeatFuture = new CompletableFuture<>();
      heartbeatFuture = newHeartbeatFuture;
      heartbeatTime = System.currentTimeMillis();
      for (MemberState member : context.getClusterState().getRemoteMemberStates()) {
        appendEntries(member);
      }
      return newHeartbeatFuture;
    }
    // If a commit future already exists, that indicates there is a heartbeat currently underway.
    // We don't want to allow callers to be completed by a heartbeat that may already almost be done.
    // So, we create the next commit future if necessary and return that. Once the current heartbeat
    // completes the next future will be used to do another heartbeat. This ensures that only one
    // heartbeat can be outstanding at any given point in time.
    else if (nextHeartbeatFuture == null) {
      nextHeartbeatFuture = new CompletableFuture<>();
      return nextHeartbeatFuture;
    } else {
      return nextHeartbeatFuture;
    }
  }

  /**
   * Commits entries up to the given index.
   * <p>
   * The returned {@link CompletableFuture} will be completed once the given index has been <em>committed</em>
   * on a majority of servers in the cluster. To determine when an index has been committed, the leader tracks
   * the {@code commitIndex} send in each {@link AppendRequest} and updates the {@link MemberState} with that
   * index upon successful {@link AppendResponse}.
   *
   * @param index The index up to which to commit entries.
   * @return A completable future to be completed once the given index has been committed on a majority
   * of followers.
   */
  public CompletableFuture<Long> commit(long index) {
    // If the index has already been completed, return a completed future.
    if (index <= context.getCompleteIndex())
      return CompletableFuture.completedFuture(index);

    // If there are no other active members in the cluster, immediately update the complete index.
    if (context.getClusterState().getActiveMemberStates().isEmpty() && context.getCommitIndex() >= index) {
      context.setCompleteIndex(index);
      return CompletableFuture.completedFuture(index);
    }

    CompletableFuture<Long> future = commitFutures.get(index);
    if (future == null) {
      future = new CompletableFuture<>();
      commitFutures.put(index, future);
    }
    return future;
  }

  /**
   * Registers a commit handler for the given commit index.
   *
   * @param index The index for which to register the handler.
   * @return A completable future to be completed once the given log index has been committed.
   */
  public CompletableFuture<Long> appendEntries(long index) {
    if (index == 0)
      return appendEntries();

    if (index <= context.getCommitIndex())
      return CompletableFuture.completedFuture(index);

    if (context.getClusterState().getActiveMemberStates().isEmpty()) {
      // If there are no other stateful servers in the cluster, immediately commit the index and update the global index.
      if (context.getClusterState().getPassiveMemberStates().isEmpty()) {
        context.setCommitIndex(index);
        context.setGlobalIndex(index);
        return CompletableFuture.completedFuture(index);
      }
      // If there are only passive members in the cluster, immediately commit the index.
      else {
        context.setCommitIndex(index);
        return CompletableFuture.completedFuture(index);
      }
    }

    // Only send entry-specific AppendRequests to active members of the cluster.
    return appendFutures.computeIfAbsent(index, i -> {
      for (MemberState member : context.getClusterState().getActiveMemberStates()) {
        appendEntries(member);
      }
      return new CompletableFuture<>();
    });
  }

  @Override
  protected void appendEntries(MemberState member) {
    // Prevent recursive, asynchronous appends from being executed if the appender has been closed.
    if (!open)
      return;

    // If prior requests to the member have failed, build an empty append request to send to the member
    // to prevent having to read from disk to configure, install, or append to an unavailable member.
    if (member.getFailureCount() > 0) {
      if (canAppend(member)) {
        sendAppendRequest(member, buildAppendEmptyRequest(member));
      }
    }
    // If the member is a reserve or passive member, send an empty AppendRequest to it.
    else if (member.getMember().type() == Member.Type.RESERVE || member.getMember().type() == Member.Type.PASSIVE) {
      if (canAppend(member)) {
        sendAppendRequest(member, buildAppendEmptyRequest(member));
      }
    }
    // If the member's current snapshot index is less than the latest snapshot index and the latest snapshot index
    // is less than the nextIndex, send a snapshot request.
    else if (member.getMember().type() == Member.Type.ACTIVE && context.getSnapshotStore().currentSnapshot() != null
      && context.getSnapshotStore().currentSnapshot().index() >= member.getNextIndex()
      && context.getSnapshotStore().currentSnapshot().index() > member.getSnapshotIndex()) {
      if (canInstall(member)) {
        sendInstallRequest(member, buildInstallRequest(member));
      }
    }
    // If no AppendRequest is already being sent, send an AppendRequest.
    else if (canAppend(member)) {
      sendAppendRequest(member, buildAppendRequest(member, context.getLog().lastIndex()));
    }
  }

  @Override
  protected boolean hasMoreEntries(MemberState member) {
    // If the member's nextIndex is an entry in the local log then more entries can be sent.
    return member.getMember().type() != Member.Type.RESERVE
      && member.getMember().type() != Member.Type.PASSIVE
      && member.getNextIndex() <= context.getLog().lastIndex();
  }

  /**
   * Returns the last time a majority of the cluster was contacted.
   * <p>
   * This is calculated by sorting the list of active members and getting the last time the majority of
   * the cluster was contacted based on the index of a majority of the members. So, in a list of 3 ACTIVE
   * members, index 1 (the second member) will be used to determine the commit time in a sorted members list.
   */
  private long majorityHeartbeatTime() {
    int quorumIndex = quorumIndex();
    if (quorumIndex >= 0) {
      return context.getClusterState().getActiveMemberStates((m1, m2)-> Long.compare(m2.getHeartbeatTime(), m1.getHeartbeatTime())).get(quorumIndex).getHeartbeatTime();
    }
    return System.currentTimeMillis();
  }

  /**
   * Sets a heartbeat time or fails the heartbeat if a quorum of successful responses cannot be achieved.
   */
  private void updateHeartbeatTime(MemberState member, Throwable error) {
    if (heartbeatFuture == null) {
      return;
    }

    if (error != null && member.getHeartbeatStartTime() == heartbeatTime) {
      int votingMemberSize = context.getClusterState().getActiveMemberStates().size() + (context.getCluster().member().type() == Member.Type.ACTIVE ? 1 : 0);
      int quorumSize = context.getClusterState().getQuorum();
      // If a quorum of successful responses cannot be achieved, fail this commit.
      if (votingMemberSize - quorumSize + 1 <= ++appendFailures) {
        heartbeatFuture.completeExceptionally(new InternalException("Failed to reach consensus"));
        completeHeartbeat();
      }
    } else {
      member.setHeartbeatTime(System.currentTimeMillis());

      // Sort the list of commit times. Use the quorum index to get the last time the majority of the cluster
      // was contacted. If the current heartbeatFuture's time is less than the commit time then trigger the
      // commit future and reset it to the next commit future.
      if (heartbeatTime <= majorityHeartbeatTime()) {
        heartbeatFuture.complete(null);
        completeHeartbeat();
      }
    }
  }

  /**
   * Completes a heartbeat by replacing the current heartbeatFuture with the nextHeartbeatFuture, updating the
   * current heartbeatTime, and starting a new {@link AppendRequest} to all active members.
   */
  private void completeHeartbeat() {
    appendFailures = 0;
    heartbeatFuture = nextHeartbeatFuture;
    nextHeartbeatFuture = null;
    if (heartbeatFuture != null) {
      heartbeatTime = System.currentTimeMillis();
      for (MemberState member : context.getClusterState().getRemoteMemberStates()) {
        appendEntries(member);
      }
    }
  }

  /**
   * Calculates the highest index that has been stored on all available, stateful servers.
   */
  private long calculateGlobalIndex() {
    // The global index is calculated by the minimum matchIndex for *all* servers in the cluster, including
    // passive members. This is critical since passive members still have state machines and thus it's still
    // important to ensure that tombstones are applied to their state machines.
    // If the members list is empty, use the local server's last log index as the global index.
    return context.getClusterState().getRemoteMemberStates().stream()
      .filter(m -> m.getMember().type() != Member.Type.RESERVE && m.getMember().status() == Member.Status.AVAILABLE)
      .mapToLong(MemberState::getMatchIndex)
      .min()
      .orElse(context.getLog().lastIndex());
  }

  /**
   * Calculates the highest index that has been stored on a majority of nodes in the current configuration.
   */
  private long calculateCommitIndex() {
    // Sort the list of replicas, order by the last index that was replicated
    // to the replica. This will allow us to determine the median index
    // for all known replicated entries across all cluster members.
    List<MemberState> members = context.getClusterState().getActiveMemberStates((m1, m2) ->
      Long.compare(m2.getMatchIndex() != 0 ? m2.getMatchIndex() : 0l, m1.getMatchIndex() != 0 ? m1.getMatchIndex() : 0l));
    if (members.isEmpty()) {
      return context.getLog().lastIndex();
    }
    return members.get(quorumIndex()).getMatchIndex();
  }

  /**
   * Calculates the highest index that has been committed on a majority of nodes in the current configuration.
   */
  private long calculateCompleteIndex() {
    // Sort the list of replicas, order by the last index that was committed
    // by the replica. This will allow us to determine the median index
    // for all known committed entries across all cluster members.
    List<MemberState> members = context.getClusterState().getActiveMemberStates((m1, m2) ->
      Long.compare(m2.getCommitIndex() != 0 ? m2.getCommitIndex() : 0l, m1.getCommitIndex() != 0 ? m1.getCommitIndex() : 0l));
    if (members.isEmpty()) {
      return context.getCommitIndex();
    }
    return members.get(quorumIndex()).getCommitIndex();
  }

  /**
   * Checks whether any futures can be completed.
   */
  private void commitEntries() {
    context.checkThread();

    // The global index may have increased even if the commit index didn't.
    long globalIndex = calculateGlobalIndex();

    // Calculate the commit index. The commit index is the highest index stored on a majority of nodes.
    long commitIndex = calculateCommitIndex();

    // Calculate the complete index. The complete index is the highest index committed on a majority of nodes.
    long completeIndex = calculateCompleteIndex();

    // If the commitIndex has not surpassed the start of the leader's term in the log, do not commit entries.
    if (leaderIndex > 0 && commitIndex >= leaderIndex) {
      // Set the global index.
      context.setGlobalIndex(globalIndex);

      // If the commit index has increased, update the commit index.
      long previousCommitIndex = context.getCommitIndex();
      if (commitIndex > 0 && commitIndex > previousCommitIndex) {
        context.setCommitIndex(commitIndex);

        // Iterate from the previous commitIndex to the updated commitIndex and remove and complete futures.
        for (long i = previousCommitIndex + 1; i <= commitIndex; i++) {
          CompletableFuture<Long> future = appendFutures.remove(i);
          if (future != null) {
            future.complete(i);
          }
        }
      }

      // If the complete index has increased, update the complete index.
      long previousCompleteIndex = context.getCompleteIndex();
      if (completeIndex > 0 && completeIndex > previousCompleteIndex) {
        context.setCompleteIndex(completeIndex);

        // Iterate from the previous completeIndex to the updated completeIndex and remove and complete futures.
        for (long i = previousCompleteIndex + 1; i <= completeIndex; i++) {
          CompletableFuture<Long> future = commitFutures.remove(i);
          if (future != null) {
            future.complete(i);
          }
        }
      }
    }
  }

  /**
   * Connects to the member and sends a commit message.
   */
  protected void sendAppendRequest(MemberState member, AppendRequest request) {
    // Set the start time of the member's current commit. This will be used to associate responses
    // with the current commit request.
    member.setHeartbeatStartTime(heartbeatTime);

    super.sendAppendRequest(member, request);
  }

  /**
   * Handles an append failure.
   */
  protected void handleAppendRequestFailure(MemberState member, AppendRequest request, Throwable error) {
    super.handleAppendRequestFailure(member, request, error);

    // Trigger commit futures if necessary.
    updateHeartbeatTime(member, error);
  }

  /**
   * Handles an append failure.
   */
  protected void handleAppendResponseFailure(MemberState member, AppendRequest request, Throwable error) {
    // Trigger commit futures if necessary.
    updateHeartbeatTime(member, error);

    super.handleAppendResponseFailure(member, request, error);
  }

  /**
   * Handles an append response.
   */
  protected void handleAppendResponse(MemberState member, AppendRequest request, AppendResponse response) {
    // Trigger commit futures if necessary.
    updateHeartbeatTime(member, null);

    super.handleAppendResponse(member, request, response);
  }

  /**
   * Handles a {@link Response.Status#OK} response.
   */
  protected void handleAppendResponseOk(MemberState member, AppendRequest request, AppendResponse response) {
    // Reset the member failure count and update the member's availability status if necessary.
    succeedAttempt(member);

    // If replication succeeded then trigger commit futures.
    if (response.succeeded()) {
      updateMatchIndex(member, response);
      updateNextIndex(member);

      // If entries were committed to the replica then check commit indexes.
      if (!request.entries().isEmpty()) {
        commitEntries();
      }

      // If there are more entries to send then attempt to send another commit.
      if (hasMoreEntries(member)) {
        appendEntries(member);
      }
    }
    // If we've received a greater term, update the term and transition back to follower.
    else if (response.term() > context.getTerm()) {
      context.setTerm(response.term()).setLeader(0);
      context.transition(CopycatServer.State.FOLLOWER);
    }
    // If the response failed, the follower should have provided the correct last index in their log. This helps
    // us converge on the matchIndex faster than by simply decrementing nextIndex one index at a time.
    else {
      resetMatchIndex(member, response);
      resetNextIndex(member);

      // If there are more entries to send then attempt to send another commit.
      if (hasMoreEntries(member)) {
        appendEntries(member);
      }
    }
  }

  /**
   * Handles a {@link Response.Status#ERROR} response.
   */
  protected void handleAppendResponseError(MemberState member, AppendRequest request, AppendResponse response) {
    // If we've received a greater term, update the term and transition back to follower.
    if (response.term() > context.getTerm()) {
      LOGGER.debug("{} - Received higher term from {}", context.getClusterState().member().address(), member.getMember().serverAddress());
      context.setTerm(response.term()).setLeader(0);
      context.transition(CopycatServer.State.FOLLOWER);
    } else {
      super.handleAppendResponseError(member, request, response);
    }
  }

  @Override
  protected void succeedAttempt(MemberState member) {
    super.succeedAttempt(member);

    // If the member is currently marked as UNAVAILABLE, change its status to AVAILABLE and update the configuration.
    if (member.getMember().status() == ServerMember.Status.UNAVAILABLE && !leader.configuring()) {
      member.getMember().update(ServerMember.Status.AVAILABLE);
      leader.configure(context.getCluster().members());
    }
  }

  @Override
  protected void failAttempt(MemberState member, Throwable error) {
    super.failAttempt(member, error);

    // Verify that the leader has contacted a majority of the cluster within the last two election timeouts.
    // If the leader is not able to contact a majority of the cluster within two election timeouts, assume
    // that a partition occurred and transition back to the FOLLOWER state.
    if (System.currentTimeMillis() - Math.max(majorityHeartbeatTime(), leaderTime) > context.getElectionTimeout().toMillis() * 2) {
      LOGGER.warn("{} - Suspected network partition. Stepping down", context.getCluster().member().address());
      context.setLeader(0);
      context.transition(CopycatServer.State.FOLLOWER);
    }
    // If the number of failures has increased above 3 and the member hasn't been marked as UNAVAILABLE, do so.
    else if (member.getFailureCount() >= 3) {
      // If the member is currently marked as AVAILABLE, change its status to UNAVAILABLE and update the configuration.
      if (member.getMember().status() == ServerMember.Status.AVAILABLE && !leader.configuring()) {
        member.getMember().update(ServerMember.Status.UNAVAILABLE);
        leader.configure(context.getCluster().members());
      }
    }
  }

  @Override
  protected void handleInstallResponse(MemberState member, InstallRequest request, InstallResponse response) {
    // Trigger commit futures if necessary.
    updateHeartbeatTime(member, null);

    super.handleInstallResponse(member, request, response);
  }

  @Override
  protected void handleInstallRequestFailure(MemberState member, InstallRequest request, Throwable error) {
    super.handleInstallRequestFailure(member, request, error);

    // Trigger commit futures if necessary.
    updateHeartbeatTime(member, error);
  }

  @Override
  protected void handleInstallResponseFailure(MemberState member, InstallRequest request, Throwable error) {
    // Trigger commit futures if necessary.
    updateHeartbeatTime(member, error);

    super.handleInstallResponseFailure(member, request, error);
  }

}
