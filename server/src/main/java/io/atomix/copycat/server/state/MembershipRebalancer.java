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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Handles rebalancing members in response to availability or membership changes.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class MembershipRebalancer {
  private static final Logger LOGGER = LoggerFactory.getLogger(MembershipRebalancer.class);
  private final ServerState context;

  MembershipRebalancer(ServerState context) {
    this.context = Assert.notNull(context, "context");
  }

  /**
   * Rebalances the cluster configuration.
   */
  public boolean rebalance(List<Member> members) {
    // The following operations are performed in a specific order such that members are properly replaced.
    // First demote any surplus active members. Active members will be demoted to reserve since demoted members
    // are not available. Then demote any surplus or unavailable passive members to reserve as well. Finally,
    // promote passive members to active to replace failed active members, and promote reserve members to
    // passive to replace promoted or unavailable passive members.
    boolean changed = demoteActiveMembers(members);
    changed = demotePassiveMembers(members) || changed;
    changed = promotePassiveMembers(members) || changed;
    changed = promoteReserveMembers(members) || changed;
    changed = promotePassiveMembers(members) || changed;
    return changed;
  }

  /**
   * Demotes active members to reserve.
   */
  private boolean demoteActiveMembers(List<Member> members) {
    boolean changed = false;

    // If the number of active members is greater than the quorum hint, demote the member with the lowest matchIndex.
    List<Member> activeMembers = members.stream().filter(Member::isActive).collect(Collectors.toList());
    if (activeMembers.size() > context.getQuorumHint()) {
      // Sort active members with the member with the lowest matchIndex first.
      Collections.sort(activeMembers, (m1, m2) -> Long.compare(context.getMemberState(m1).getMatchIndex(), context.getMemberState(m2).getMatchIndex()));

      int demoteTotal = activeMembers.size() - context.getQuorumHint();
      int demoteCount = 0;

      for (Member member : activeMembers) {
        MemberState state = context.getMemberState(member);
        if (state.getStatus() == MemberState.Status.UNAVAILABLE) {
          members.remove(member);
          members.add(new Member(Member.Type.RESERVE, member.serverAddress(), member.clientAddress()));
          LOGGER.debug("{} - Demoted active member {} to reserve", context.getMember().serverAddress(), member.serverAddress());
          changed = true;
          if (++demoteCount == demoteTotal) {
            break;
          }
        }
      }
    }
    return changed;
  }

  /**
   * Demotes passive members to reserve.
   */
  private boolean demotePassiveMembers(List<Member> members) {
    boolean changed = false;

    List<Member> reserveMembers = members.stream().filter(Member::isReserve).collect(Collectors.toList());

    long availableReserves = reserveMembers.stream().filter(m -> context.getMemberState(m).getStatus() == MemberState.Status.AVAILABLE).count();

    // Sort the passive members list with the members with the lowest matchIndex first.
    List<Member> passiveMembers = members.stream().filter(Member::isPassive).collect(Collectors.toList());
    Collections.sort(passiveMembers, (m1, m2) -> Long.compare(context.getMemberState(m1).getMatchIndex(), context.getMemberState(m2).getMatchIndex()));

    // Iterate through passive members. For any member that is UNAVAILABLE, demote the member so long
    // as a RESERVE member is AVAILABLE to replace it, ensuring that the number of passive members isn't
    // unnecessarily decreased below the desired number of passive members.
    Iterator<Member> iterator = passiveMembers.iterator();
    int passiveCount = passiveMembers.size();
    while (iterator.hasNext() && passiveCount + availableReserves >= context.getQuorumHint() + context.getBackupCount()) {
      Member member = iterator.next();
      MemberState state = context.getMemberState(member);
      if (state.getStatus() == MemberState.Status.UNAVAILABLE) {
        members.remove(member);
        members.add(new Member(Member.Type.RESERVE, member.serverAddress(), member.clientAddress()));
        passiveCount--;
        LOGGER.debug("{} - Demoted passive member {} to reserve", context.getMember().serverAddress(), member.serverAddress());
        changed = true;
      }
    }
    return changed;
  }

  /**
   * Promotes passive members to active.
   */
  private boolean promotePassiveMembers(List<Member> members) {
    // Promote PASSIVE members and then RESERVE members to ACTIVE. This ensures that reserve members can
    // be immediately promoted to ACTIVE if necessary.
    return promoteToActive(Member.Type.PASSIVE, members) || promoteToActive(Member.Type.RESERVE, members);
  }

  /**
   * Promotes members to active.
   */
  private boolean promoteToActive(Member.Type type, List<Member> members) {
    boolean changed = false;

    // Sort the members with the member with the highest matchIndex first.
    List<Member> filteredMembers = members.stream().filter(m -> m.type() == type).collect(Collectors.toList());
    Collections.sort(filteredMembers, (m1, m2) -> Long.compare(context.getMemberState(m1).getCommitIndex(), context.getMemberState(m2).getCommitIndex()));

    // Iterate through members in the promotable members list and add AVAILABLE members to the ACTIVE members
    // list until the quorum hint is reached.
    Iterator<Member> iterator = filteredMembers.iterator();
    long activeMembers = members.stream().filter(Member::isActive).count();
    while (activeMembers < context.getQuorumHint() && iterator.hasNext()) {
      Member member = iterator.next();
      MemberState state = context.getMemberState(member);
      if (state.getStatus() == MemberState.Status.AVAILABLE) {
        members.remove(member);
        members.add(new Member(Member.Type.ACTIVE, member.serverAddress(), member.clientAddress()));
        activeMembers++;
        LOGGER.debug("{} - Promoted {} member {} to active", context.getMember().serverAddress(), type.toString().toLowerCase(), member.serverAddress());
        changed = true;
      }
    }
    return changed;
  }

  /**
   * Promotes reserve members to passive.
   */
  private boolean promoteReserveMembers(List<Member> members) {
    boolean changed = false;

    // Iterate through members in the reserve members list and add AVAILABLE members to the PASSIVE members
    // list until the number of passive members is equal to the product of the quorum hint and the backup count.
    long passiveMembers = members.stream().filter(Member::isActive).count();
    Iterator<Member> iterator = members.stream().filter(Member::isReserve).collect(Collectors.toList()).iterator();
    while (passiveMembers < context.getQuorumHint() * context.getBackupCount() && iterator.hasNext()) {
      Member member = iterator.next();
      MemberState state = context.getMemberState(member);
      if (state.getStatus() == MemberState.Status.AVAILABLE) {
        members.remove(member);
        members.add(new Member(Member.Type.PASSIVE, member.serverAddress(), member.clientAddress()));
        passiveMembers++;
        LOGGER.debug("{} - Promoted reserve member {} to passive", context.getMember().serverAddress(), member.serverAddress());
        changed = true;
      }
    }
    return changed;
  }

}
