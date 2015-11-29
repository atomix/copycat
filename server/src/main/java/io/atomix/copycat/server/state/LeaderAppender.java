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
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.cluster.MemberContext;
import io.atomix.copycat.server.cluster.RaftMemberType;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.response.AppendResponse;
import io.atomix.copycat.server.storage.entry.Entry;

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
  private static final int MAX_BATCH_SIZE = 1024 * 32;
  private final LeaderState leader;
  private final long leaderTime;
  private final long leaderIndex;
  private long commitTime;
  private int commitFailures;
  private CompletableFuture<Long> commitFuture;
  private CompletableFuture<Long> nextCommitFuture;
  private final Map<Long, CompletableFuture<Long>> commitFutures = new HashMap<>();

  LeaderAppender(LeaderState leader) {
    super(leader.controller);
    this.leader = Assert.notNull(leader, "leader");
    this.leaderTime = System.currentTimeMillis();
    this.leaderIndex = controller.context().getLog().nextIndex();
    this.commitTime = leaderTime;
  }

  /**
   * Returns the current commit time.
   *
   * @return The current commit time.
   */
  public long time() {
    return commitTime;
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
    return controller.context().getCluster().getQuorum() - 2;
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
    if (controller.context().getCluster().getRemoteMemberStates().size() == 0)
      return CompletableFuture.completedFuture(null);

    // If no commit future already exists, that indicates there's no heartbeat currently under way.
    // Create a new commit future and commit to all members in the cluster.
    if (commitFuture == null) {
      commitFuture = new CompletableFuture<>();
      commitTime = System.currentTimeMillis();
      for (MemberContext member : controller.context().getCluster().getRemoteMemberStates()) {
        appendEntries(member);
      }
      return commitFuture;
    }
    // If a commit future already exists, that indicates there is a heartbeat currently underway.
    // We don't want to allow callers to be completed by a heartbeat that may already almost be done.
    // So, we create the next commit future if necessary and return that. Once the current heartbeat
    // completes the next future will be used to do another heartbeat. This ensures that only one
    // heartbeat can be outstanding at any given point in time.
    else if (nextCommitFuture == null) {
      nextCommitFuture = new CompletableFuture<>();
      return nextCommitFuture;
    } else {
      return nextCommitFuture;
    }
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

    // If there are no other servers in the cluster, immediately commit the index.
    if (controller.context().getCluster().getRemoteMemberStates().isEmpty()) {
      controller.context().setCommitIndex(index);
      controller.context().setGlobalIndex(index);
      return CompletableFuture.completedFuture(index);
    }
    // If there are no other active members in the cluster, update the commit index and complete
    // the commit but ensure append entries requests are sent to passive members.
    else if (controller.context().getCluster().getVotingMemberStates().isEmpty()) {
      controller.context().setCommitIndex(index);
      for (MemberContext member : controller.context().getCluster().getRemoteMemberStates()) {
        appendEntries(member);
      }
      return CompletableFuture.completedFuture(index);
    }

    // Ensure append requests are being sent to all members, including passive members.
    return commitFutures.computeIfAbsent(index, i -> {
      for (MemberContext member : controller.context().getCluster().getRemoteMemberStates()) {
        appendEntries(member);
      }
      return new CompletableFuture<>();
    });
  }

  /**
   * Returns the last time a majority of the cluster was contacted.
   * <p>
   * This is calculated by sorting the list of active members and getting the last time the majority of
   * the cluster was contacted based on the index of a majority of the members. So, in a list of 3 ACTIVE
   * members, index 1 (the second member) will be used to determine the commit time in a sorted members list.
   */
  private long commitTime() {
    int quorumIndex = quorumIndex();
    if (quorumIndex >= 0) {
      return controller.context().getCluster().getVotingMemberStates((m1, m2) -> Long.compare(m2.getCommitTime(), m1.getCommitTime())).get(quorumIndex).getCommitTime();
    }
    return System.currentTimeMillis();
  }

  /**
   * Sets a commit time or fails the commit if a quorum of successful responses cannot be achieved.
   */
  private void commitTime(MemberContext member, Throwable error) {
    if (commitFuture == null) {
      return;
    }

    if (error != null && member.getCommitStartTime() == this.commitTime) {
      int votingMemberSize = controller.context().getCluster().getVotingMemberStates().size() + (controller.context().getCluster().getMember().type().isVoting() ? 1 : 0);
      int quorumSize = controller.context().getCluster().getQuorum();
      // If a quorum of successful responses cannot be achieved, fail this commit.
      if (votingMemberSize - quorumSize + 1 <= ++commitFailures) {
        commitFuture.completeExceptionally(new InternalException("Failed to reach consensus"));
        completeCommit();
      }
    } else {
      member.setCommitTime(System.currentTimeMillis());

      // Sort the list of commit times. Use the quorum index to get the last time the majority of the cluster
      // was contacted. If the current commitFuture's time is less than the commit time then trigger the
      // commit future and reset it to the next commit future.
      if (this.commitTime <= commitTime()) {
        commitFuture.complete(null);
        completeCommit();
      }
    }
  }

  /**
   * Completes a heartbeat by replacing the current commitFuture with the nextCommitFuture, updating the
   * current commitTime, and starting a new {@link AppendRequest} to all active members.
   */
  private void completeCommit() {
    commitFailures = 0;
    commitFuture = nextCommitFuture;
    nextCommitFuture = null;
    if (commitFuture != null) {
      this.commitTime = System.currentTimeMillis();
      for (MemberContext replica : controller.context().getCluster().getRemoteMemberStates()) {
        appendEntries(replica);
      }
    }
  }

  /**
   * Checks whether any futures can be completed.
   */
  private void commitEntries() {
    controller.context().checkThread();

    // The global index may have increased even if the commit index didn't. Update the global index.
    // The global index is calculated by the minimum matchIndex for *all* servers in the cluster, including
    // passive members. This is critical since passive members still have state machines and thus it's still
    // important to ensure that tombstones are applied to their state machines.
    // If the members list is empty, use the local server's last log index as the global index.
    long globalMatchIndex = controller.context().getCluster().getRemoteMemberStates().stream().mapToLong(MemberContext::getMatchIndex).min().orElse(controller.context().getLog().lastIndex());
    controller.context().setGlobalIndex(globalMatchIndex);

    // Sort the list of replicas, order by the last index that was replicated
    // to the replica. This will allow us to determine the median index
    // for all known replicated entries across all cluster members.
    List<MemberContext> members = controller.context().getCluster().getVotingMemberStates((m1, m2) ->
      Long.compare(m2.getMatchIndex() != 0 ? m2.getMatchIndex() : 0l, m1.getMatchIndex() != 0 ? m1.getMatchIndex() : 0l));

    // If the active members list is empty (a configuration change occurred between an append request/response)
    // ensure all commit futures are completed and cleared.
    if (members.isEmpty()) {
      controller.context().setCommitIndex(controller.context().getLog().lastIndex());
      for (Map.Entry<Long, CompletableFuture<Long>> entry : commitFutures.entrySet()) {
        entry.getValue().complete(entry.getKey());
      }
      commitFutures.clear();
      return;
    }

    // Calculate the current commit index as the median matchIndex.
    long commitIndex = members.get(quorumIndex()).getMatchIndex();

    // If the commit index has increased then update the commit index. Note that in order to ensure
    // the leader completeness property holds, verify that the commit index is greater than or equal to
    // the index of the leader's no-op entry. Update the commit index and trigger commit futures.
    long previousCommitIndex = controller.context().getCommitIndex();
    if (commitIndex > 0 && commitIndex > previousCommitIndex && (leaderIndex > 0 && commitIndex >= leaderIndex)) {
      controller.context().setCommitIndex(commitIndex);

      // Iterate from the previous commitIndex to the updated commitIndex and remove and complete futures.
      for (long i = previousCommitIndex + 1; i <= commitIndex; i++) {
        CompletableFuture<Long> future = commitFutures.remove(i);
        if (future != null) {
          future.complete(i);
        }
      }
    }
  }

  @Override
  protected AppendRequest buildAppendRequest(MemberContext member) {
    // If the log is empty then send an empty commit.
    // If the member is not stateful then send an empty commit if two election timeouts have passed.
    // If the next index hasn't yet been set then we send an empty commit first.
    // If the next index is greater than the last index then send an empty commit.
    // If the member failed to respond to recent communication send an empty commit. This
    // helps avoid doing expensive work until we can ascertain the member is back up.
    if (!member.getMember().type().isStateful()) {
      if (member.getMember().status() == Member.Status.UNAVAILABLE || System.currentTimeMillis() - member.getCommitTime() > controller.context().getElectionTimeout().toMillis() * 2) {
        return buildEmptyRequest(member);
      }
    } else {
      if (controller.context().getLog().isEmpty() || member.getNextIndex() > controller.context().getLog().lastIndex() || member.getFailureCount() > 0) {
        return buildEmptyRequest(member);
      } else {
        return buildEntryRequest(member);
      }
    }
    return buildEntryRequest(member);
  }

  /**
   * Builds an empty AppendEntries request.
   * <p>
   * Empty append requests are used as heartbeats to followers.
   */
  private AppendRequest buildEmptyRequest(MemberContext member) {
    long prevIndex = getPrevIndex(member);
    Entry prevEntry = getPrevEntry(member, prevIndex);

    return AppendRequest.builder()
      .withTerm(controller.context().getTerm())
      .withLeader(controller.context().getCluster().getMember().id())
      .withLogIndex(prevIndex)
      .withLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
      .withCommitIndex(controller.context().getCommitIndex())
      .withGlobalIndex(controller.context().getGlobalIndex())
      .build();
  }

  /**
   * Builds a populated AppendEntries request.
   */
  private AppendRequest buildEntryRequest(MemberContext member) {
    long prevIndex = getPrevIndex(member);
    Entry prevEntry = getPrevEntry(member, prevIndex);

    AppendRequest.Builder builder = AppendRequest.builder()
      .withTerm(controller.context().getTerm())
      .withLeader(controller.context().getCluster().getMember().id())
      .withLogIndex(prevIndex)
      .withLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
      .withCommitIndex(controller.context().getCommitIndex())
      .withGlobalIndex(controller.context().getGlobalIndex());

    // Build a list of entries to send to the member.
    if (!controller.context().getLog().isEmpty()) {
      long index = prevIndex != 0 ? prevIndex + 1 : controller.context().getLog().firstIndex();

      // We build a list of entries up to the MAX_BATCH_SIZE. Note that entries in the log may
      // be null if they've been compacted and the member to which we're sending entries is just
      // joining the cluster or is otherwise far behind. Null entries are simply skipped and not
      // counted towards the size of the batch.
      int size = 0;
      while (index <= controller.context().getLog().lastIndex()) {
        // Get the entry from the log and append it if it's not null. Entries in the log can be null
        // if they've been cleaned or compacted from the log. Each entry sent in the append request
        // has a unique index to handle gaps in the log.
        Entry entry = controller.context().getLog().get(index);
        if (entry != null) {
          if (size + entry.size() > MAX_BATCH_SIZE) {
            break;
          }
          size += entry.size();
          builder.addEntry(entry);
        }
        index++;
      }
    }

    // Release the previous entry back to the entry pool.
    if (prevEntry != null) {
      prevEntry.release();
    }

    return builder.build();
  }

  @Override
  protected void startAppendRequest(MemberContext member, AppendRequest request) {
    super.startAppendRequest(member, request);
    member.setCommitStartTime(commitTime);
  }

  @Override
  protected void endAppendRequest(MemberContext member, AppendRequest request, Throwable error) {
    super.endAppendRequest(member, request, error);
    commitTime(member, error);
  }

  @Override
  protected void handleAppendResponse(MemberContext member, AppendRequest request, AppendResponse response) {
    if (response.status() == Response.Status.OK) {
      handleAppendResponseOk(member, request, response);
    } else {
      handleAppendResponseError(member, request, response);
    }
  }

  /**
   * Handles a {@link Response.Status#OK} response.
   */
  private void handleAppendResponseOk(MemberContext member, AppendRequest request, AppendResponse response) {
    succeedAttempt(member);

    // Update the commit time for the replica. This will cause heartbeat futures to be triggered.
    commitTime(member, null);

    // If replication succeeded then trigger commit futures.
    if (response.succeeded()) {
      updateMatchIndex(member, response);
      updateNextIndex(member);
      updateConfiguration(member);

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
    else if (response.term() > controller.context().getTerm()) {
      controller.context().setTerm(response.term()).setLeader(0);
      controller.transition(RaftStateType.FOLLOWER);
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
  private void handleAppendResponseError(MemberContext member, AppendRequest request, AppendResponse response) {
    // If we've received a greater term, update the term and transition back to follower.
    if (response.term() > controller.context().getTerm()) {
      LOGGER.debug("{} - Received higher term from {}", controller.context().getCluster().getMember().serverAddress(), member.getMember().serverAddress());
      controller.context().setTerm(response.term()).setLeader(0);
      controller.transition(RaftStateType.FOLLOWER);
    } else {
      // If any other error occurred, increment the failure count for the member. Log the first three failures,
      // and thereafter log 1% of the failures. This keeps the log from filling up with annoying error messages
      // when attempting to send entries to down followers.
      int failures = member.incrementFailureCount();
      if (failures <= 3 || failures % 100 == 0) {
        LOGGER.warn("{} - AppendRequest to {} failed. Reason: [{}]", controller.context().getCluster().getMember().serverAddress(), member.getMember().serverAddress(), response.error() != null ? response.error() : "");
      }
    }
  }

  @Override
  protected void handleAppendError(MemberContext member, AppendRequest request, Throwable error) {
    failAttempt(member, error);
  }

  /**
   * Succeeds an attempt to contact a member.
   */
  private void succeedAttempt(MemberContext member) {
    // Reset the member failure count.
    member.resetFailureCount();

    // If the member is currently marked as UNAVAILABLE, change its status to AVAILABLE and update the configuration.
    if (member.getMember().status() == Member.Status.UNAVAILABLE && !leader.configuring()) {
      member.getMember().update(Member.Status.AVAILABLE);
      leader.configure(controller.context().getCluster().getMembers());
    }
  }

  /**
   * Fails an attempt to contact a member.
   */
  private void failAttempt(MemberContext member, Throwable error) {
    // If any append error occurred, increment the failure count for the member. Log the first three failures,
    // and thereafter log 1% of the failures. This keeps the log from filling up with annoying error messages
    // when attempting to send entries to down followers.
    int failures = member.incrementFailureCount();
    if (failures <= 3 || failures % 100 == 0) {
      LOGGER.warn("{} - {}", controller.context().getCluster().getMember().serverAddress(), error.getMessage());
    }

    // Verify that the leader has contacted a majority of the cluster within the last two election timeouts.
    // If the leader is not able to contact a majority of the cluster within two election timeouts, assume
    // that a partition occurred and transition back to the FOLLOWER state.
    if (System.currentTimeMillis() - Math.max(commitTime(), leaderTime) > controller.context().getElectionTimeout().toMillis() * 2) {
      LOGGER.warn("{} - Suspected network partition. Stepping down", controller.context().getCluster().getMember().serverAddress());
      controller.context().setLeader(0);
      controller.transition(RaftStateType.FOLLOWER);
    }
    // If the number of failures has increased above 3 and the member hasn't been marked as UNAVAILABLE, do so.
    else if (failures >= 3) {
      // If the member is currently marked as AVAILABLE, change its status to UNAVAILABLE and update the configuration.
      if (member.getMember().status() == Member.Status.AVAILABLE && !leader.configuring()) {
        member.getMember().update(Member.Status.UNAVAILABLE);
        leader.configure(controller.context().getCluster().getMembers());
      }
    }
  }

  /**
   * Updates the cluster configuration for the given member.
   */
  private void updateConfiguration(MemberContext member) {
    if (member.getMember().type() == RaftMemberType.PASSIVE && !leader.configuring() && member.getMatchIndex() >= controller.context().getCommitIndex()) {
      member.getMember().update(RaftMemberType.ACTIVE);
      leader.configure(controller.context().getCluster().getMembers());
    }
  }

}
