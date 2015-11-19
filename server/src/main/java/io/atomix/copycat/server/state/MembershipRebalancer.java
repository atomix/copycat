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

/**
 * Handles rebalancing members.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class MembershipRebalancer {
  private static final Logger LOGGER = LoggerFactory.getLogger(MembershipRebalancer.class);
  private final ServerState context;

  MembershipRebalancer(ServerState context) {
    this.context = Assert.notNull(context, "context");
  }

  /**
   * Rebalances the cluster configuration.
   */
  public boolean rebalance(List<Member> activeMembers, List<Member> passiveMembers, List<Member> reserveMembers) {
    // The following operations are performed in a specific order such that members are properly replaced.
    // First demote any surplus active members. Active members will be demoted to reserve since demoted members
    // are not available. Then demote any surplus or unavailable passive members to reserve as well. Finally,
    // promote passive members to active to replace failed active members, and promote reserve members to
    // passive to replace promoted or unavailable passive members.
    boolean changed = demoteActiveMembers(activeMembers, passiveMembers, reserveMembers);
    changed = demotePassiveMembers(activeMembers, passiveMembers, reserveMembers) || changed;
    changed = promotePassiveMembers(activeMembers, passiveMembers, reserveMembers) || changed;
    changed = promoteReserveMembers(activeMembers, passiveMembers, reserveMembers) || changed;
    changed = promotePassiveMembers(activeMembers, passiveMembers, reserveMembers) || changed;
    return changed;
  }

  /**
   * Demotes active members to reserve.
   */
  private boolean demoteActiveMembers(List<Member> activeMembers, List<Member> passiveMembers, List<Member> reserveMembers) {
    boolean changed = false;

    // If the number of active members is greater than the quorum hint, demote the member with the lowest matchIndex.
    if (activeMembers.size() > context.getQuorumHint()) {
      // Sort active members with the member with the lowest matchIndex first.
      Collections.sort(activeMembers, (m1, m2) -> Long.compare(context.getMemberState(m1).getMatchIndex(), context.getMemberState(m2).getMatchIndex()));

      int demoteTotal = activeMembers.size() - context.getQuorumHint();
      int demoteCount = 0;

      Iterator<Member> iterator = activeMembers.iterator();
      while (iterator.hasNext()) {
        Member member = iterator.next();
        MemberState state = context.getMemberState(member);
        if (state.getStatus() == MemberState.Status.UNAVAILABLE) {
          iterator.remove();
          reserveMembers.add(member);
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
  private boolean demotePassiveMembers(List<Member> activeMembers, List<Member> passiveMembers, List<Member> reserveMembers) {
    boolean changed = false;

    long availableReserves = reserveMembers.stream().filter(m -> context.getMemberState(m).getStatus() == MemberState.Status.AVAILABLE).count();

    // Sort the passive members list with the members with the lowest matchIndex first.
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
        iterator.remove();
        reserveMembers.add(member);
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
  private boolean promotePassiveMembers(List<Member> activeMembers, List<Member> passiveMembers, List<Member> reserveMembers) {
    // Promote PASSIVE members and then RESERVE members to ACTIVE. This ensures that reserve members can
    // be immediately promoted to ACTIVE if necessary.
    return promoteToActive(passiveMembers, activeMembers) || promoteToActive(reserveMembers, activeMembers);
  }

  /**
   * Promotes members to active.
   */
  private boolean promoteToActive(List<Member> members, List<Member> activeMembers) {
    boolean changed = false;

    // Sort the members with the member with the highest matchIndex first.
    Collections.sort(members, (m1, m2) -> Long.compare(context.getMemberState(m1).getMatchIndex(), context.getMemberState(m2).getMatchIndex()));

    // Iterate through members in the promotable members list and add AVAILABLE members to the ACTIVE members
    // list until the quorum hint is reached.
    Iterator<Member> iterator = members.iterator();
    while (activeMembers.size() < context.getQuorumHint() && iterator.hasNext()) {
      Member member = iterator.next();
      MemberState state = context.getMemberState(member);
      if (state.getStatus() == MemberState.Status.AVAILABLE) {
        iterator.remove();
        activeMembers.add(member);
        LOGGER.debug("{} - Promoted {} member {} to active", context.getMember().serverAddress(), state.getType().toString().toLowerCase(), member.serverAddress());
        changed = true;
      }
    }
    return changed;
  }

  /**
   * Promotes reserve members to passive.
   */
  private boolean promoteReserveMembers(List<Member> activeMembers, List<Member> passiveMembers, List<Member> reserveMembers) {
    boolean changed = false;

    // Iterate through members in the reserve members list and add AVAILABLE members to the PASSIVE members
    // list until the number of passive members is equal to the product of the quorum hint and the backup count.
    Iterator<Member> iterator = reserveMembers.iterator();
    while (passiveMembers.size() < context.getQuorumHint() * context.getBackupCount() && iterator.hasNext()) {
      Member member = iterator.next();
      MemberState state = context.getMemberState(member);
      if (state.getStatus() == MemberState.Status.AVAILABLE) {
        iterator.remove();
        passiveMembers.add(member);
        LOGGER.debug("{} - Promoted reserve member {} to passive", context.getMember().serverAddress(), member.serverAddress());
        changed = true;
      }
    }
    return changed;
  }

}
