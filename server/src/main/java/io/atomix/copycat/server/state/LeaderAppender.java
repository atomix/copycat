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

import io.atomix.copycat.client.error.InternalException;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.response.AppendResponse;
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * Sends AppendEntries RPCs for leaders.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class LeaderAppender extends AbstractAppender {
  private static final int MAX_BATCH_SIZE = 1024 * 32;
  private final long leaderTime;
  private final long leaderIndex;
  private long commitTime;
  private int commitFailures;
  private CompletableFuture<Long> commitFuture;
  private CompletableFuture<Long> nextCommitFuture;
  private final TreeMap<Long, CompletableFuture<Long>> commitFutures = new TreeMap<>();

  LeaderAppender(ServerState context) {
    super(context);
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
    return context.getQuorum() - 2;
  }

  /**
   * Triggers a commit.
   *
   * @return A completable future to be completed the next time entries are committed to a majority of the cluster.
   */
  public CompletableFuture<Long> appendEntries() {
    if (context.getActiveMemberStates().size() == 0)
      return CompletableFuture.completedFuture(null);

    // If no commit future already exists, that indicates there's no heartbeat currently under way.
    // Create a new commit future and commit to all members in the cluster.
    if (commitFuture == null) {
      commitFuture = new CompletableFuture<>();
      commitTime = System.currentTimeMillis();
      for (MemberState member : context.getActiveMemberStates()) {
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

    // If there are no other ACTIVE members in the cluster, immediately commit the index.
    if (context.getActiveMemberStates().isEmpty()) {
      context.setCommitIndex(index);
      return CompletableFuture.completedFuture(index);
    }

    // Ensure append requests are being sent to all ACTIVE members.
    return commitFutures.computeIfAbsent(index, i -> {
      for (MemberState member : context.getActiveMemberStates()) {
        appendEntries(member);
      }
      return new CompletableFuture<>();
    });
  }

  /**
   * Returns the last time a majority of the cluster was contacted.
   */
  private long commitTime() {
    int quorumIndex = quorumIndex();
    if (quorumIndex >= 0) {
      return context.getActiveMemberStates((m1, m2) -> Long.compare(m2.getCommitTime() > 0 ? m2.getCommitTime() : System.currentTimeMillis(), m1.getCommitTime() > 0 ? m1.getCommitTime() : System.currentTimeMillis())).get(quorumIndex).getCommitTime();
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

    boolean completed = false;
    if (error != null && member.getCommitStartTime() == this.commitTime) {
      int activeMemberSize = context.getActiveMemberStates().size() + (context.getMemberState().getMember().isActive() ? 1 : 0);
      int quorumSize = context.getQuorum();
      // If a quorum of successful responses cannot be achieved, fail this commit.
      if (activeMemberSize - quorumSize + 1 <= ++commitFailures) {
        commitFuture.completeExceptionally(new InternalException("Failed to reach consensus"));
        completed = true;
      }
    } else {
      member.setCommitTime(System.currentTimeMillis());

      // Sort the list of commit times. Use the quorum index to get the last time the majority of the cluster
      // was contacted. If the current commitFuture's time is less than the commit time then trigger the
      // commit future and reset it to the next commit future.
      if (this.commitTime <= commitTime()) {
        commitFuture.complete(null);
        completed = true;
      }
    }

    if (completed) {
      commitFailures = 0;
      commitFuture = nextCommitFuture;
      nextCommitFuture = null;
      if (commitFuture != null) {
        this.commitTime = System.currentTimeMillis();
        for (MemberState replica : context.getActiveMemberStates()) {
          appendEntries(replica);
        }
      }
    }
  }

  /**
   * Checks whether any futures can be completed.
   */
  private void commitEntries() {
    context.checkThread();

    // Sort the list of replicas, order by the last index that was replicated to the replica. This will allow
    // us to determine the median index for all known replicated entries across all cluster members.
    // Note that this sort should be fast in most cases since the list should already be sorted, but there
    // may still be a more efficient way to go about this.
    List<MemberState> members = context.getActiveMemberStates((m1, m2) ->
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
    if (commitIndex > 0 && commitIndex > context.getCommitIndex() && (leaderIndex > 0 && commitIndex >= leaderIndex)) {
      context.setCommitIndex(commitIndex);

      // TODO: This seems like an annoyingly expensive operation to perform on every response.
      // Futures could simply be stored in a hash map and we could use a sequential index starting
      // from the previous commit index to get the appropriate futures. But for now, at least this
      // ensures that no memory leaks can occur.
      SortedMap<Long, CompletableFuture<Long>> futures = commitFutures.headMap(commitIndex, true);
      for (Map.Entry<Long, CompletableFuture<Long>> entry : futures.entrySet()) {
        entry.getValue().complete(entry.getKey());
      }
      futures.clear();
    }
  }

  @Override
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
   */
  private AppendRequest buildEmptyRequest(MemberState member) {
    long prevIndex = getPrevIndex(member);
    Entry prevEntry = getPrevEntry(member, prevIndex);

    return AppendRequest.builder()
      .withTerm(context.getTerm())
      .withLeader(context.getMember().id())
      .withLogIndex(prevIndex)
      .withLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
      .withCommitIndex(context.getCommitIndex())
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
      .withLeader(context.getMember().id())
      .withLogIndex(prevIndex)
      .withLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
      .withCommitIndex(context.getCommitIndex());

    // Build a list of entries to send to the member.
    if (!context.getLog().isEmpty()) {
      long index = prevIndex != 0 ? prevIndex + 1 : context.getLog().firstIndex();

      // We build a list of entries up to the MAX_BATCH_SIZE. Note that entries in the log may
      // be null if they've been compacted and the member to which we're sending entries is just
      // joining the cluster or is otherwise far behind. Null entries are simply skipped and not
      // counted towards the size of the batch.
      int size = 0;
      while (index <= context.getLog().lastIndex()) {
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

  @Override
  protected void startAppendRequest(MemberState member, AppendRequest request) {
    super.startAppendRequest(member, request);
    member.setCommitStartTime(commitTime);
  }

  @Override
  protected void endAppendRequest(MemberState member, AppendRequest request, Throwable error) {
    super.endAppendRequest(member, request, error);
    commitTime(member, error);
  }

  @Override
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
    // Reset the member failure count.
    member.resetFailureCount();

    // Update the commit time for the replica. This will cause heartbeat futures to be triggered.
    commitTime(member, null);

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
    } else if (response.term() > context.getTerm()) {
      context.setTerm(response.term()).setLeader(0);
      context.transition(CopycatServer.State.FOLLOWER);
    } else {
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
    if (response.term() > context.getTerm()) {
      LOGGER.debug("{} - Received higher term from {}", context.getMember().serverAddress(), member.getMember().serverAddress());
      context.setTerm(response.term()).setLeader(0);
      context.transition(CopycatServer.State.FOLLOWER);
    } else {
      int failures = member.incrementFailureCount();
      if (failures <= 3 || failures % 100 == 0) {
        LOGGER.warn("{} - AppendRequest to {} failed. Reason: [{}]", context.getMember().serverAddress(), member.getMember().serverAddress(), response.error() != null ? response.error() : "");
      }
    }
  }

  @Override
  protected void handleAppendError(MemberState member, AppendRequest request, Throwable error) {
    failAttempt(member, error);
  }

  /**
   * Fails an attempt to contact a member.
   */
  private void failAttempt(MemberState member, Throwable error) {
    int failures = member.incrementFailureCount();
    if (failures <= 3 || failures % 100 == 0) {
      LOGGER.warn("{} - {}", context.getMember().serverAddress(), error.getMessage());
    }

    // Verify that the leader has contacted a majority of the cluster within the last two election timeouts.
    // If the leader is not able to contact a majority of the cluster within two election timeouts, assume
    // that a partition occurred and transition back to the FOLLOWER state.
    if (System.currentTimeMillis() - Math.max(commitTime(), leaderTime) > context.getElectionTimeout().toMillis() * 2) {
      LOGGER.warn("{} - Suspected network partition. Stepping down", context.getMember().serverAddress());
      context.setLeader(0);
      context.transition(CopycatServer.State.FOLLOWER);
    }
  }

}
