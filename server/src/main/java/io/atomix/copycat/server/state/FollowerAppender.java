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

import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.response.AppendResponse;
import io.atomix.copycat.server.storage.entry.ConfigurationEntry;
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.List;
import java.util.function.Predicate;

/**
 * Sends AppendEntries RPCs for a follower.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class FollowerAppender extends AbstractAppender {
  private static final int MAX_BATCH_SIZE = 1024 * 32;
  private static final Predicate<Entry> PASSIVE_ENTRY_PREDICATE = e -> true;
  private static final Predicate<Entry> RESERVE_ENTRY_PREDICATE = e -> e instanceof ConfigurationEntry;

  public FollowerAppender(ServerState context) {
    super(context);
  }

  /**
   * Sends append entries requests to passive/reserve members.
   */
  public void appendEntries() {
    appendEntries(context.getAssignedPassiveMemberStates());
    appendEntries(context.getAssignedReserveMemberStates());
  }

  /**
   * Sends append entries requests for the given members.
   */
  private void appendEntries(List<MemberState> members) {
    for (MemberState member : members) {
      appendEntries(member);
    }
  }

  @Override
  protected AppendRequest buildAppendRequest(MemberState member) {
    // Send append entries RPCs to the member according to its type. PASSIVE members receive all entries,
    // and RESERVE members receive only configuration changes.
    if (member.getMember().isPassive()) {
      return buildPassiveRequest(member);
    } else {
      return buildReserveRequest(member);
    }
  }

  /**
   * Builds a passive member append request.
   */
  private AppendRequest buildPassiveRequest(MemberState member) {
    // If a RESERVE member's matchIndex is less than the current commit index, send entries to the member.
    if (!context.getLog().isEmpty() && member.getMatchIndex() < context.getCommitIndex()) {
      return buildRequest(member, PASSIVE_ENTRY_PREDICATE);
    }
    return null;
  }

  /**
   * Builds a reserve member append request.
   */
  private AppendRequest buildReserveRequest(MemberState member) {
    // If a RESERVE member's matchIndex is less than the current configuration version, send entries to the member.
    if (!context.getLog().isEmpty() && member.getMatchIndex() < context.getVersion()) {
      return buildRequest(member, RESERVE_ENTRY_PREDICATE);
    }
    return null;
  }

  /**
   * Builds an append request, filtering log entries by the given predicate.
   */
  private AppendRequest buildRequest(MemberState member, Predicate<Entry> filter) {
    // Send as many entries as possible to the member. We use the basic mechanism of AppendEntries RPCs
    // as described in the Raft literature.
    // Note that we don't need to account for members that are unavailable with empty append requests since the
    // heartbeat mechanism handles availability for us. Thus, we can always safely attempt to send as many entries
    // as possible without incurring too much additional overhead.
    long prevIndex = getPrevIndex(member);
    Entry prevEntry = getPrevEntry(member, prevIndex);

    Member leader = context.getLeader();
    AppendRequest.Builder builder = AppendRequest.builder()
      .withTerm(context.getTerm())
      .withLeader(leader != null ? leader.id() : 0)
      .withLogIndex(prevIndex)
      .withLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
      .withCommitIndex(context.getCommitIndex());

    // Build a list of entries to send to the member.
    long index = prevIndex != 0 ? prevIndex + 1 : context.getLog().firstIndex();

    // We build a list of entries up to the MAX_BATCH_SIZE. Note that entries in the log may
    // be null if they've been compacted and the member to which we're sending entries is just
    // joining the cluster or is otherwise far behind. Null entries are simply skipped and not
    // counted towards the size of the batch.
    int size = 0;
    while (index <= context.getCommitIndex()) {
      Entry entry = context.getLog().get(index);
      if (entry != null && filter.test(entry)) {
        if (size + entry.size() > MAX_BATCH_SIZE) {
          break;
        }
        size += entry.size();
        builder.addEntry(entry);
      }
      index++;
    }

    // Release the previous entry back to the entry pool.
    if (prevEntry != null) {
      prevEntry.release();
    }

    return builder.build();
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

    // If replication succeeded then trigger commit futures.
    if (response.succeeded()) {
      updateMatchIndex(member, response);
      updateNextIndex(member);

      // If there are more entries to send then attempt to send another commit.
      if (!request.entries().isEmpty() && hasMoreEntries(member)) {
        appendEntries(member);
      }
    } else {
      // If the response term is greater than the local term, increment it.
      if (response.term() > context.getTerm()) {
        context.setTerm(response.term());
      }

      // Reset the match and next indexes according to the response.
      resetMatchIndex(member, response);
      resetNextIndex(member);

      // If there are more entries to send then attempt to send another commit.
      if (!request.entries().isEmpty() && hasMoreEntries(member)) {
        appendEntries(member);
      }
    }
  }

  /**
   * Handles a {@link Response.Status#ERROR} response.
   */
  private void handleAppendResponseError(MemberState member, AppendRequest request, AppendResponse response) {
    // If the response term is greater than the local term, increment it.
    if (response.term() > context.getTerm()) {
      context.setTerm(response.term());
    }

    // Log 1% of append failures to reduce logging.
    if (member.incrementFailureCount() % 100 == 0) {
      LOGGER.warn("{} - AppendRequest to {} failed. Reason: [{}]", context.getMember().serverAddress(), member.getMember().serverAddress(), response.error() != null ? response.error() : "");
    }
  }

  @Override
  protected void handleAppendError(MemberState member, AppendRequest request, Throwable error) {
    // Ignore errors.
  }

}
