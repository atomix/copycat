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

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.buffer.HeapBuffer;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.error.InternalException;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.request.ConfigureRequest;
import io.atomix.copycat.server.request.InstallRequest;
import io.atomix.copycat.server.response.AppendResponse;
import io.atomix.copycat.server.response.ConfigureResponse;
import io.atomix.copycat.server.response.InstallResponse;
import io.atomix.copycat.server.storage.entry.Entry;
import io.atomix.copycat.server.storage.snapshot.Snapshot;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

/**
 * The leader appender is responsible for sending {@link AppendRequest}s on behalf of a leader to followers.
 * Append requests are sent by the leader only to other active members of the cluster.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class LeaderAppender implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(LeaderAppender.class);
  private static final int MAX_BATCH_SIZE = 1024 * 32;
  private final LeaderState leader;
  protected final ServerState context;
  private final Set<MemberState> appending = new HashSet<>();
  private final Set<MemberState> configuring = new HashSet<>();
  private final Set<MemberState> installing = new HashSet<>();
  private final long leaderTime;
  private final long leaderIndex;
  private long commitTime;
  private int commitFailures;
  private CompletableFuture<Long> commitFuture;
  private CompletableFuture<Long> nextCommitFuture;
  private final Map<Long, CompletableFuture<Long>> commitFutures = new HashMap<>();
  private boolean open = true;

  LeaderAppender(LeaderState leader) {
    this.leader = Assert.notNull(leader, "leader");
    this.context = leader.context;
    this.leaderTime = System.currentTimeMillis();
    this.leaderIndex = context.getLog().nextIndex();
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
    return context.getCluster().getQuorum() - 2;
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
    if (context.getCluster().getRemoteMemberStates().size() == 0)
      return CompletableFuture.completedFuture(null);

    // If no commit future already exists, that indicates there's no heartbeat currently under way.
    // Create a new commit future and commit to all members in the cluster.
    if (commitFuture == null) {
      CompletableFuture<Long> newCommitFuture = new CompletableFuture<>();
      commitFuture = newCommitFuture;
      commitTime = System.currentTimeMillis();
      for (MemberState member : context.getCluster().getRemoteMemberStates()) {
        appendEntries(member);
      }
      return newCommitFuture;
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

    if (index <= context.getCommitIndex())
      return CompletableFuture.completedFuture(index);

    // If there are no other servers in the cluster, immediately commit the index.
    if (context.getCluster().getRemoteMemberStates().isEmpty()) {
      context.setCommitIndex(index);
      context.setGlobalIndex(index);
      return CompletableFuture.completedFuture(index);
    }
    // If there are no other active members in the cluster, update the commit index and complete
    // the commit but ensure append entries requests are sent to passive members.
    else if (context.getCluster().getRemoteMemberStates(CopycatServer.Type.ACTIVE).isEmpty()) {
      context.setCommitIndex(index);
      for (MemberState member : context.getCluster().getRemoteMemberStates()) {
        appendEntries(member);
      }
      return CompletableFuture.completedFuture(index);
    }

    // Ensure append requests are being sent to all members, including passive members.
    return commitFutures.computeIfAbsent(index, i -> {
      for (MemberState member : context.getCluster().getRemoteMemberStates()) {
        appendEntries(member);
      }
      return new CompletableFuture<>();
    });
  }

  /**
   * Sends append entries requests to the given member.
   */
  protected void appendEntries(MemberState member) {
    // Prevent recursive, asynchronous appends from being executed if the appender has been closed.
    if (!open)
      return;

    // If the member term is less than the current term or the member's configuration index is less
    // than the local configuration index, send a configuration update to the member.
    // Ensure that only one configuration attempt per member is attempted at any given time by storing the
    // member state in a set of configuring members.
    // Once the configuration is complete sendAppendRequest will be called recursively.
    if (member.getConfigTerm() < context.getTerm() || member.getConfigIndex() < context.getCluster().getVersion()) {
      configure(member);
    }
    // If the member's current snapshot index is less than the latest snapshot index and the latest snapshot index
    // is less than the nextIndex, send a snapshot request.
    else if (context.getSnapshotStore().currentSnapshot() != null
      && context.getSnapshotStore().currentSnapshot().index() >= member.getNextIndex()
      && context.getSnapshotStore().currentSnapshot().index() > member.getSnapshotIndex()) {
      install(member);
    }
    // If no AppendRequest is already being sent, send an AppendRequest.
    else if (!appending.contains(member)) {
      AppendRequest request = buildAppendRequest(member);
      if (request != null) {
        sendAppendRequest(member, request);
      }
    }
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
      return context.getCluster().getRemoteMemberStates(CopycatServer.Type.ACTIVE, (m1, m2)-> Long.compare(m2.getCommitTime(), m1.getCommitTime())).get(quorumIndex).getCommitTime();
    }
    return System.currentTimeMillis();
  }

  /**
   * Sets a commit time or fails the commit if a quorum of successful responses cannot be achieved.
   */
  private void commitTime(MemberState member, Throwable error) {
    if (commitFuture == null) {
      return;
    }

    if (error != null && member.getCommitStartTime() == commitTime) {
      int votingMemberSize = context.getCluster().getRemoteMemberStates(CopycatServer.Type.ACTIVE).size() + (context.getCluster().getMember().type() == CopycatServer.Type.ACTIVE ? 1 : 0);
      int quorumSize = context.getCluster().getQuorum();
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
      if (commitTime <= commitTime()) {
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
      commitTime = System.currentTimeMillis();
      for (MemberState replica : context.getCluster().getRemoteMemberStates()) {
        appendEntries(replica);
      }
    }
  }

  /**
   * Checks whether any futures can be completed.
   */
  private void commitEntries() {
    context.checkThread();

    // The global index may have increased even if the commit index didn't. Update the global index.
    // The global index is calculated by the minimum matchIndex for *all* servers in the cluster, including
    // passive members. This is critical since passive members still have state machines and thus it's still
    // important to ensure that tombstones are applied to their state machines.
    // If the members list is empty, use the local server's last log index as the global index.
    long globalMatchIndex = context.getCluster().getRemoteMemberStates().stream().mapToLong(MemberState::getMatchIndex).min().orElse(context.getLog().lastIndex());
    context.setGlobalIndex(globalMatchIndex);

    // Sort the list of replicas, order by the last index that was replicated
    // to the replica. This will allow us to determine the median index
    // for all known replicated entries across all cluster members.
    List<MemberState> members = context.getCluster().getRemoteMemberStates(CopycatServer.Type.ACTIVE, (m1, m2)->
      Long.compare(m2.getMatchIndex() != 0 ? m2.getMatchIndex() : 0l, m1.getMatchIndex() != 0 ? m1.getMatchIndex() : 0l));

    // If the active members list is empty (a configuration change occurred between an append request/response)
    // ensure all commit futures are completed and cleared.
    if (members.isEmpty()) {
      context.setCommitIndex(context.getLog().lastIndex());
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
    long previousCommitIndex = context.getCommitIndex();
    if (commitIndex > 0 && commitIndex > previousCommitIndex && (leaderIndex > 0 && commitIndex >= leaderIndex)) {
      context.setCommitIndex(commitIndex);

      // Iterate from the previous commitIndex to the updated commitIndex and remove and complete futures.
      for (long i = previousCommitIndex + 1; i <= commitIndex; i++) {
        CompletableFuture<Long> future = commitFutures.remove(i);
        if (future != null) {
          future.complete(i);
        }
      }
    }
  }

  /**
   * Builds an append request.
   *
   * @param member The member to which to send the request.
   * @return The append request.
   */
  protected AppendRequest buildAppendRequest(MemberState member) {
    // If the log is empty then send an empty commit.
    // If the next index hasn't yet been set then we send an empty commit first.
    // If the next index is greater than the last index then send an empty commit.
    // If the member failed to respond to recent communication send an empty commit. This
    // helps avoid doing expensive work until we can ascertain the member is back up.
    if (context.getLog().isEmpty() || member.getNextIndex() > context.getLog().lastIndex() || member.getFailureCount() > 0) {
      return buildEmptyRequest(member);
    } else {
      return buildEntryRequest(member);
    }
  }

  /**
   * Builds an empty AppendEntries request.
   * <p>
   * Empty append requests are used as heartbeats to followers.
   */
  private AppendRequest buildEmptyRequest(MemberState member) {
    long prevIndex = getPrevIndex(member);
    Entry prevEntry = getPrevEntry(member, prevIndex);

    return AppendRequest.builder()
      .withTerm(context.getTerm())
      .withLeader(context.getCluster().getMember().id())
      .withLogIndex(prevIndex)
      .withLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
      .withCommitIndex(context.getCommitIndex())
      .withGlobalIndex(context.getGlobalIndex())
      .build();
  }

  /**
   * Builds a populated AppendEntries request.
   */
  private AppendRequest buildEntryRequest(MemberState member) {
    long prevIndex = getPrevIndex(member);
    Entry prevEntry = getPrevEntry(member, prevIndex);

    AppendRequest.Builder builder = AppendRequest.builder()
      .withTerm(context.getTerm())
      .withLeader(context.getCluster().getMember().id())
      .withLogIndex(prevIndex)
      .withLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
      .withCommitIndex(context.getCommitIndex())
      .withGlobalIndex(context.getGlobalIndex());

    // Build a list of entries to send to the member.
    if (!context.getLog().isEmpty()) {
      long index = prevIndex != 0 ? prevIndex + 1 : context.getLog().firstIndex();

      // We build a list of entries up to the MAX_BATCH_SIZE. Note that entries in the log may
      // be null if they've been compacted and the member to which we're sending entries is just
      // joining the cluster or is otherwise far behind. Null entries are simply skipped and not
      // counted towards the size of the batch.
      int size = 0;
      while (index <= context.getLog().lastIndex()) {
        // Get the entry from the log and append it if it's not null. Entries in the log can be null
        // if they've been cleaned or compacted from the log. Each entry sent in the append request
        // has a unique index to handle gaps in the log.
        Entry entry = context.getLog().get(index);
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

  /**
   * Gets the previous index.
   */
  protected long getPrevIndex(MemberState member) {
    return member.getNextIndex() - 1;
  }

  /**
   * Gets the previous entry.
   */
  protected Entry getPrevEntry(MemberState member, long prevIndex) {
    if (prevIndex > 0) {
      return context.getLog().get(prevIndex);
    }
    return null;
  }

  /**
   * Connects to the member and sends a commit message.
   */
  protected void sendAppendRequest(MemberState member, AppendRequest request) {
    // Add the member to the appending set to prevent concurrent append attempts to the member.
    appending.add(member);

    // Set the start time of the member's current commit. This will be used to associate responses
    // with the current commit request.
    member.setCommitStartTime(commitTime);

    LOGGER.debug("{} - Sent {} to {}", context.getCluster().getMember().serverAddress(), request, member.getMember().serverAddress());
    context.getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendAppendRequest(connection, member, request);
        } else {
          // Remove the member from the appending set to allow the next append request.
          appending.remove(member);

          // Trigger commit futures if necessary.
          commitTime(member, error);

          // Log the failed attempt to contact the member.
          failAttempt(member, error);
        }
      }
    });
  }

  /**
   * Sends a commit message.
   */
  protected void sendAppendRequest(Connection connection, MemberState member, AppendRequest request) {
    connection.<AppendRequest, AppendResponse>send(request).whenComplete((response, error) -> {
      context.checkThread();

      // Remove the member from the appending list to allow the next append request.
      appending.remove(member);

      // Trigger commit futures if necessary.
      commitTime(member, error);

      if (open) {
        if (error == null) {
          LOGGER.debug("{} - Received {} from {}", context.getCluster().getMember().serverAddress(), response, member.getMember().serverAddress());
          handleAppendResponse(member, request, response);
        } else {
          // Log the failed attempt to contact the member.
          failAttempt(member, error);
        }
      }
    });
  }

  /**
   * Handles an append response.
   */
  protected void handleAppendResponse(MemberState member, AppendRequest request, AppendResponse response) {
    if (response.status() == Response.Status.OK) {
      handleAppendResponseOk(member, request, response);
    } else {
      handleAppendResponseError(member, request, response);
    }
  }

  /**
   * Handles a {@link Response.Status#OK} response.
   */
  private void handleAppendResponseOk(MemberState member, AppendRequest request, AppendResponse response) {
    // Reset the member failure count and update the member's availability status if necessary.
    succeedAttempt(member);

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
  private void handleAppendResponseError(MemberState member, AppendRequest request, AppendResponse response) {
    // If we've received a greater term, update the term and transition back to follower.
    if (response.term() > context.getTerm()) {
      LOGGER.debug("{} - Received higher term from {}", context.getCluster().getMember().serverAddress(), member.getMember().serverAddress());
      context.setTerm(response.term()).setLeader(0);
      context.transition(CopycatServer.State.FOLLOWER);
    } else {
      // If any other error occurred, increment the failure count for the member. Log the first three failures,
      // and thereafter log 1% of the failures. This keeps the log from filling up with annoying error messages
      // when attempting to send entries to down followers.
      int failures = member.incrementFailureCount();
      if (failures <= 3 || failures % 100 == 0) {
        LOGGER.warn("{} - AppendRequest to {} failed. Reason: [{}]", context.getCluster().getMember().serverAddress(), member.getMember().serverAddress(), response.error() != null ? response.error() : "");
      }
    }
  }

  /**
   * Succeeds an attempt to contact a member.
   */
  private void succeedAttempt(MemberState member) {
    // Reset the member failure count.
    member.resetFailureCount();

    // If the member is currently marked as UNAVAILABLE, change its status to AVAILABLE and update the configuration.
    if (member.getMember().status() == Member.Status.UNAVAILABLE && !leader.configuring()) {
      member.getMember().update(Member.Status.AVAILABLE);
      leader.configure(context.getCluster().getMembers());
    }
  }

  /**
   * Fails an attempt to contact a member.
   */
  private void failAttempt(MemberState member, Throwable error) {
    // Reset the connection to the given member to ensure failed connections are reconstructed upon retries.
    context.getConnections().resetConnection(member.getMember().serverAddress());

    // If any append error occurred, increment the failure count for the member. Log the first three failures,
    // and thereafter log 1% of the failures. This keeps the log from filling up with annoying error messages
    // when attempting to send entries to down followers.
    int failures = member.incrementFailureCount();
    if (failures <= 3 || failures % 100 == 0) {
      LOGGER.warn("{} - {}", context.getCluster().getMember().serverAddress(), error.getMessage());
    }

    // Verify that the leader has contacted a majority of the cluster within the last two election timeouts.
    // If the leader is not able to contact a majority of the cluster within two election timeouts, assume
    // that a partition occurred and transition back to the FOLLOWER state.
    if (System.currentTimeMillis() - Math.max(commitTime(), leaderTime) > context.getElectionTimeout().toMillis() * 2) {
      LOGGER.warn("{} - Suspected network partition. Stepping down", context.getCluster().getMember().serverAddress());
      context.setLeader(0);
      context.transition(CopycatServer.State.FOLLOWER);
    }
    // If the number of failures has increased above 3 and the member hasn't been marked as UNAVAILABLE, do so.
    else if (failures >= 3) {
      // If the member is currently marked as AVAILABLE, change its status to UNAVAILABLE and update the configuration.
      if (member.getMember().status() == Member.Status.AVAILABLE && !leader.configuring()) {
        member.getMember().update(Member.Status.UNAVAILABLE);
        leader.configure(context.getCluster().getMembers());
      }
    }
  }

  /**
   * Updates the cluster configuration for the given member.
   */
  private void updateConfiguration(MemberState member) {
    if (member.getMember().type() == CopycatServer.Type.PASSIVE && !leader.configuring() && member.getMatchIndex() >= context.getCommitIndex()) {
      member.getMember().update(CopycatServer.Type.ACTIVE);
      leader.configure(context.getCluster().getMembers());
    }
  }

  /**
   * Returns a boolean value indicating whether there are more entries to send.
   */
  protected boolean hasMoreEntries(MemberState member) {
    // If the member's nextIndex is an entry in the local log then more entries can be sent.
    return member.getNextIndex() <= context.getLog().lastIndex();
  }

  /**
   * Updates the match index when a response is received.
   */
  protected void updateMatchIndex(MemberState member, AppendResponse response) {
    // If the replica returned a valid match index then update the existing match index.
    member.setMatchIndex(response.logIndex());
  }

  /**
   * Updates the next index when the match index is updated.
   */
  protected void updateNextIndex(MemberState member) {
    // If the match index was set, update the next index to be greater than the match index if necessary.
    member.setNextIndex(Math.max(member.getNextIndex(), Math.max(member.getMatchIndex() + 1, 1)));
  }

  /**
   * Resets the match index when a response fails.
   */
  protected void resetMatchIndex(MemberState member, AppendResponse response) {
    member.setMatchIndex(response.logIndex());
    LOGGER.debug("{} - Reset match index for {} to {}", context.getCluster().getMember().serverAddress(), member, member.getMatchIndex());
  }

  /**
   * Resets the next index when a response fails.
   */
  protected void resetNextIndex(MemberState member) {
    if (member.getMatchIndex() != 0) {
      member.setNextIndex(member.getMatchIndex() + 1);
    } else {
      member.setNextIndex(context.getLog().firstIndex());
    }
    LOGGER.debug("{} - Reset next index for {} to {}", context.getCluster().getMember().serverAddress(), member, member.getNextIndex());
  }

  /**
   * Sends a configuration to the given member.
   */
  protected void configure(MemberState member) {
    if (!configuring.contains(member)) {
      ConfigureRequest request = buildConfigureRequest(member);
      if (request != null) {
        sendConfigureRequest(member, request);
      }
    }
  }

  /**
   * Builds a configure request for the given member.
   */
  protected ConfigureRequest buildConfigureRequest(MemberState member) {
    Member leader = context.getLeader();
    return ConfigureRequest.builder()
      .withTerm(context.getTerm())
      .withLeader(leader != null ? leader.id() : 0)
      .withIndex(context.getCluster().getVersion())
      .withMembers(context.getCluster().getMembers())
      .build();
  }

  /**
   * Connects to the member and sends a configure request.
   */
  protected void sendConfigureRequest(MemberState member, ConfigureRequest request) {
    // Add the member to the configuring set to prevent concurrent configure attempts.
    configuring.add(member);

    context.getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendConfigureRequest(connection, member, request);
        } else {
          // Remove the member from the configuring set to allow a new configure request.
          configuring.remove(member);

          // Trigger commit futures if necessary.
          commitTime(member, error);

          // Log the failed attempt to contact the member.
          failAttempt(member, error);
        }
      }
    });
  }

  /**
   * Sends a configuration message.
   */
  protected void sendConfigureRequest(Connection connection, MemberState member, ConfigureRequest request) {
    LOGGER.debug("{} - Sent {} to {}", context.getCluster().getMember().serverAddress(), request, member.getMember().serverAddress());
    connection.<ConfigureRequest, ConfigureResponse>send(request).whenComplete((response, error) -> {
      context.checkThread();

      // Remove the member from the configuring list to allow a new configure request.
      configuring.remove(member);

      // Trigger commit futures if necessary.
      commitTime(member, error);

      if (open) {
        if (error == null) {
          LOGGER.debug("{} - Received {} from {}", context.getCluster().getMember().serverAddress(), response, member.getMember().serverAddress());
          handleConfigureResponse(member, request, response);
        } else {
          LOGGER.warn("{} - Failed to configure {}", context.getCluster().getMember().serverAddress(), member.getMember().serverAddress());

          // Log the failed attempt to contact the member.
          failAttempt(member, error);
        }
      }
    });
  }

  /**
   * Handles a configuration response.
   */
  protected void handleConfigureResponse(MemberState member, ConfigureRequest request, ConfigureResponse response) {
    if (response.status() == Response.Status.OK) {
      handleConfigureResponseOk(member, request, response);
    } else {
      handleConfigureResponseError(member, request, response);
    }
  }

  /**
   * Handles an OK configuration response.
   */
  protected void handleConfigureResponseOk(MemberState member, ConfigureRequest request, ConfigureResponse response) {
    // Reset the member failure count and update the member's status if necessary.
    succeedAttempt(member);

    // Update the member's current configuration term and index according to the installed configuration.
    member.setConfigTerm(request.term()).setConfigIndex(request.index());

    // Recursively append entries to the member.
    appendEntries(member);
  }

  /**
   * Handles an ERROR configuration response.
   */
  protected void handleConfigureResponseError(MemberState member, ConfigureRequest request, ConfigureResponse response) {
    appendEntries(member);
  }

  /**
   * Sends a snapshot to the given member.
   */
  protected void install(MemberState member) {
    if (!installing.contains(member)) {
      InstallRequest request = buildInstallRequest(member);
      if (request != null) {
        sendInstallRequest(member, request);
      }
    }
  }

  /**
   * Builds an install request for the given member.
   */
  protected InstallRequest buildInstallRequest(MemberState member) {
    Snapshot snapshot = context.getSnapshotStore().currentSnapshot();
    if (member.getNextSnapshotIndex() != snapshot.index()) {
      member.setNextSnapshotIndex(snapshot.index()).setNextSnapshotOffset(0);
    }

    InstallRequest request;
    synchronized (snapshot) {
      // Open a new snapshot reader.
      try (SnapshotReader reader = snapshot.reader()) {
        // Skip to the next batch of bytes according to the snapshot chunk size and current offset.
        reader.skip(member.getNextSnapshotOffset() * MAX_BATCH_SIZE);
        Buffer buffer = HeapBuffer.allocate(Math.min(MAX_BATCH_SIZE, reader.remaining()));
        reader.read(buffer);

        // Create the install request, indicating whether this is the last chunk of data based on the number
        // of bytes remaining in the buffer.
        Member leader = context.getLeader();
        request = InstallRequest.builder()
          .withTerm(context.getTerm())
          .withLeader(leader != null ? leader.id() : 0)
          .withIndex(member.getNextSnapshotIndex())
          .withOffset(member.getNextSnapshotOffset())
          .withData(buffer.flip())
          .withComplete(!reader.hasRemaining())
          .build();
      }
    }

    return request;
  }

  /**
   * Connects to the member and sends a snapshot request.
   */
  protected void sendInstallRequest(MemberState member, InstallRequest request) {
    // Add the member to the installing set to prevent concurrent install attempts.
    installing.add(member);

    context.getConnections().getConnection(member.getMember().serverAddress()).whenComplete((connection, error) -> {
      context.checkThread();

      if (open) {
        if (error == null) {
          sendInstallRequest(connection, member, request);
        } else {
          // Remove the member from the installing set to allow a new install request.
          installing.remove(member);

          // Trigger commit futures if necessary.
          commitTime(member, error);

          // Log the failed attempt to contact the member.
          failAttempt(member, error);
        }
      }
    });
  }

  /**
   * Sends a snapshot message.
   */
  protected void sendInstallRequest(Connection connection, MemberState member, InstallRequest request) {
    LOGGER.debug("{} - Sent {} to {}", context.getCluster().getMember().serverAddress(), request, member.getMember().serverAddress());
    connection.<InstallRequest, InstallResponse>send(request).whenComplete((response, error) -> {
      context.checkThread();

      // Remove the member from the installing list to allow a new install request.
      installing.remove(member);

      // Trigger commit futures if necessary.
      commitTime(member, error);

      if (open) {
        if (error == null) {
          LOGGER.debug("{} - Received {} from {}", context.getCluster().getMember().serverAddress(), response, member.getMember().serverAddress());
          handleInstallResponse(member, request, response);
        } else {
          LOGGER.warn("{} - Failed to install {}", context.getCluster().getMember().serverAddress(), member.getMember().serverAddress());

          // Reset the member's snapshot index and offset to resend the snapshot from the start
          // once a connection to the member is re-established.
          member.setNextSnapshotIndex(0).setNextSnapshotOffset(0);

          // Log the failed attempt to contact the member.
          failAttempt(member, error);
        }
      }
    });
  }

  /**
   * Handles an install response.
   */
  protected void handleInstallResponse(MemberState member, InstallRequest request, InstallResponse response) {
    if (response.status() == Response.Status.OK) {
      handleInstallResponseOk(member, request, response);
    } else {
      handleInstallResponseError(member, request, response);
    }
  }

  /**
   * Handles an OK install response.
   */
  protected void handleInstallResponseOk(MemberState member, InstallRequest request, InstallResponse response) {
    // Reset the member failure count and update the member's status if necessary.
    succeedAttempt(member);

    // If the install request was completed successfully, set the member's snapshotIndex and reset
    // the next snapshot index/offset.
    if (request.complete()) {
      member.setSnapshotIndex(request.index())
        .setNextSnapshotIndex(0)
        .setNextSnapshotOffset(0);
    }
    // If more install requests remain, increment the member's snapshot offset.
    else {
      member.setNextSnapshotOffset(request.offset() + 1);
    }

    // Recursively append entries to the member.
    appendEntries(member);
  }

  /**
   * Handles an ERROR install response.
   */
  protected void handleInstallResponseError(MemberState member, InstallRequest request, InstallResponse response) {
    LOGGER.warn("{} - Failed to install {}", context.getCluster().getMember().serverAddress(), member.getMember().serverAddress());
    member.setNextSnapshotIndex(0).setNextSnapshotOffset(0);
  }

  @Override
  public void close() {
    open = false;
  }

}
