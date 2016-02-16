/*
 * Copyright 2016 the original author or authors.
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

import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.ArrayList;
import java.util.List;

/**
 * Follower appender.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class FollowerAppender extends AbstractAppender {

  public FollowerAppender(ServerContext context) {
    super(context);
  }

  /**
   * Sends append requests to assigned passive members.
   */
  public void appendEntries() {
    if (open) {
      for (MemberState member : context.getClusterState().getAssignedPassiveMemberStates()) {
        appendEntries(member);
      }
      for (MemberState member : context.getClusterState().getAssignedReserveMemberStates()) {
        appendEntries(member);
      }
    }
  }

  @Override
  protected boolean hasMoreEntries(MemberState member) {
    return (member.getMember().type() == Member.Type.PASSIVE && member.getNextIndex() <= context.getCommitIndex())
      || (member.getMember().type() == Member.Type.RESERVE && context.getClusterState().getConfiguration().index() < context.getCommitIndex() && member.getNextIndex() < context.getClusterState().getConfiguration().index());
  }

  @Override
  protected void appendEntries(MemberState member) {
    // Prevent recursive, asynchronous appends from being executed if the appender has been closed.
    if (!open)
      return;

    // If the member's current snapshot index is less than the latest snapshot index and the latest snapshot index
    // is less than the nextIndex, send a snapshot request.
    if (context.getSnapshotStore().currentSnapshot() != null
      && context.getSnapshotStore().currentSnapshot().index() >= member.getNextIndex()
      && context.getSnapshotStore().currentSnapshot().index() > member.getSnapshotIndex()) {
      if (canInstall(member)) {
        sendInstallRequest(member, buildInstallRequest(member));
      }
    }
    // If no AppendRequest is already being sent, send an AppendRequest.
    else if (canAppend(member) && hasMoreEntries(member)) {
      sendAppendRequest(member, buildAppendRequest(member, context.getCommitIndex()));
    }
  }

  /**
   * Builds an append request.
   *
   * @param member The member to which to send the request.
   * @return The append request.
   */
  protected AppendRequest buildAppendRequest(MemberState member, long lastIndex) {
    // If the log is empty then send an empty commit.
    // If the next index hasn't yet been set then we send an empty commit first.
    // If the next index is greater than the last index then send an empty commit.
    // If the member failed to respond to recent communication send an empty commit. This
    // helps avoid doing expensive work until we can ascertain the member is back up.
    if (member.getMember().type() == Member.Type.PASSIVE) {
      if (context.getLog().isEmpty() || member.getNextIndex() > lastIndex || member.getFailureCount() > 0) {
        return buildAppendEmptyRequest(member);
      } else {
        return buildAppendEntriesRequest(member, lastIndex);
      }
    } else if (member.getMember().type() == Member.Type.RESERVE) {
      if (context.getClusterState().getConfiguration().index() < context.getCommitIndex() && member.getNextIndex() < context.getClusterState().getConfiguration().index() && member.getFailureCount() == 0) {
        return buildAppendConfigurationRequest(member);
      } else {
        return buildAppendEmptyRequest(member);
      }
    }
    return buildAppendEmptyRequest(member);
  }

  /**
   * Builds a populated AppendEntries request.
   */
  @SuppressWarnings("unchecked")
  protected AppendRequest buildAppendConfigurationRequest(MemberState member) {
    Entry prevEntry = getPrevEntry(member);

    ServerMember leader = context.getLeader();
    AppendRequest.Builder builder = AppendRequest.builder()
      .withTerm(context.getTerm())
      .withLeader(leader != null ? leader.id() : 0)
      .withLogIndex(prevEntry != null ? prevEntry.getIndex() : 0)
      .withLogTerm(prevEntry != null ? prevEntry.getTerm() : 0)
      .withCommitIndex(context.getCommitIndex())
      .withGlobalIndex(context.getGlobalIndex());

    // Build a list of entries to send to the member.
    List<Entry> entries = new ArrayList<>(1);
    Entry entry = context.getLog().get(context.getClusterState().getConfiguration().index());
    if (entry != null) {
      entries.add(entry);
    }

    // Release the previous entry back to the entry pool.
    if (prevEntry != null) {
      prevEntry.release();
    }

    // Add the entries to the request builder and build the request.
    return builder.withEntries(entries).build();
  }

}
