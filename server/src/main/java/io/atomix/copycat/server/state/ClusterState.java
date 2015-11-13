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

import io.atomix.catalyst.util.Assert;

import java.util.*;

/**
 * Cluster state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ClusterState {
  private final ServerState context;
  private final Member member;
  private Type type = Type.RESERVE;
  private long version = -1;
  private final Map<Integer, MemberState> membersMap = new HashMap<>();
  private final Map<Integer, Type> types = new HashMap<>();
  private final List<MemberState> members = new ArrayList<>();
  private final List<MemberState> activeMembers = new ArrayList<>();
  private final List<MemberState> passiveMembers = new ArrayList<>();
  private final List<MemberState> reserveMembers = new ArrayList<>();

  /**
   * Member state type.
   */
  enum Type {
    ACTIVE,
    PASSIVE,
    RESERVE
  }

  ClusterState(ServerState context, Member member) {
    this.context = Assert.notNull(context, "context");
    this.member = Assert.notNull(member, "member");
  }

  /**
   * Returns the local cluster member.
   *
   * @return The local cluster member.
   */
  Member getMember() {
    return member;
  }

  /**
   * Returns the local member type.
   *
   * @return The local member type.
   */
  Type getType() {
    return type;
  }

  /**
   * Sets the local member type.
   *
   * @param type The local member type.
   * @return The cluster state.
   */
  ClusterState setType(Type type) {
    this.type = Assert.notNull(type, "type");
    return this;
  }

  /**
   * Returns a boolean indicating whether the local node is active.
   *
   * @return Whether the local node is active.
   */
  boolean isActive() {
    return type == Type.ACTIVE;
  }

  /**
   * Returns a boolean indicating whether the local node is passive.
   *
   * @return Whether the local node is passive.
   */
  boolean isPassive() {
    return type == Type.PASSIVE;
  }

  /**
   * Returns a boolean indicating whether the local node is reserve.
   *
   * @return Whether the local node is reserve.
   */
  boolean isReserve() {
    return type == Type.RESERVE;
  }

  /**
   * Returns the remote quorum count.
   *
   * @return The remote quorum count.
   */
  int getQuorum() {
    return (int) Math.floor((activeMembers.size() + 1) / 2.0) + 1;
  }

  /**
   * Returns the cluster state version.
   *
   * @return The cluster state version.
   */
  long getVersion() {
    return version;
  }

  /**
   * Clears all members from the cluster state.
   *
   * @return The cluster state.
   */
  private ClusterState clearMembers() {
    members.clear();
    activeMembers.clear();
    passiveMembers.clear();
    membersMap.clear();
    types.clear();
    return this;
  }

  /**
   * Returns a member by ID.
   *
   * @param id The member ID.
   * @return The member state.
   */
  MemberState getMember(int id) {
    return membersMap.get(id);
  }

  /**
   * Returns a boolean value indicating whether the given member is active.
   *
   * @param member The member state.
   * @return Indicates whether the member is active.
   */
  boolean isActiveMember(MemberState member) {
    return types.get(member.getServerAddress().hashCode()) == Type.ACTIVE;
  }

  /**
   * Returns a boolean value indicating whether the given member is passive.
   *
   * @param member The member state.
   * @return Indicates whether the member is passive.
   */
  boolean isPassiveMember(MemberState member) {
    return types.get(member.getServerAddress().hashCode()) == Type.PASSIVE;
  }

  /**
   * Returns a list of passive members.
   *
   * @return A list of passive members.
   */
  List<MemberState> getPassiveMembers() {
    return passiveMembers;
  }

  /**
   * Returns a list of passive members.
   *
   * @param comparator A comparator with which to sort the members list.
   * @return The sorted members list.
   */
  List<MemberState> getPassiveMembers(Comparator<MemberState> comparator) {
    Collections.sort(passiveMembers, comparator);
    return passiveMembers;
  }

  /**
   * Returns a list of active members.
   *
   * @return A list of active members.
   */
  List<MemberState> getActiveMembers() {
    return activeMembers;
  }

  /**
   * Returns a list of active members.
   *
   * @param comparator A comparator with which to sort the members list.
   * @return The sorted members list.
   */
  List<MemberState> getActiveMembers(Comparator<MemberState> comparator) {
    Collections.sort(activeMembers, comparator);
    return activeMembers;
  }

  /**
   * Returns a list of all members.
   *
   * @return A list of all members.
   */
  List<MemberState> getMembers() {
    return members;
  }

  /**
   * Configures the cluster state.
   *
   * @param version The cluster state version.
   * @param activeMembers The active members.
   * @param passiveMembers The passive members.
   * @param reserveMembers The reserve members.
   * @return The cluster state.
   */
  ClusterState configure(long version, Collection<Member> activeMembers, Collection<Member> passiveMembers, Collection<Member> reserveMembers) {
    if (version <= this.version)
      return this;

    List<MemberState> newActiveMembers = buildMembers(activeMembers);
    List<MemberState> newPassiveMembers = buildMembers(passiveMembers);
    List<MemberState> newReserveMembers = buildMembers(reserveMembers);

    clearMembers();

    for (MemberState member : newActiveMembers) {
      membersMap.put(member.getServerAddress().hashCode(), member);
      members.add(member);
      this.activeMembers.add(member);
      types.put(member.getServerAddress().hashCode(), Type.ACTIVE);
    }

    for (MemberState member : newPassiveMembers) {
      membersMap.put(member.getServerAddress().hashCode(), member);
      members.add(member);
      this.passiveMembers.add(member);
      types.put(member.getServerAddress().hashCode(), Type.PASSIVE);
    }

    for (MemberState member : newReserveMembers) {
      membersMap.put(member.getServerAddress().hashCode(), member);
      members.add(member);
      this.reserveMembers.add(member);
      types.put(member.getServerAddress().hashCode(), Type.RESERVE);
    }

    if (activeMembers.contains(member)) {
      type = Type.ACTIVE;
    } else if (passiveMembers.contains(member)) {
      type = Type.PASSIVE;
    } else if (reserveMembers.contains(member)) {
      type = Type.RESERVE;
    } else {
      type = null;
    }

    this.version = version;

    return this;
  }

  /**
   * Builds a list of all members.
   */
  Collection<Member> buildMembers() {
    List<Member> members = new ArrayList<>();
    for (MemberState member : activeMembers) {
      members.add(new Member(member.getServerAddress(), member.getClientAddress()));
    }
    for (MemberState member : passiveMembers) {
      members.add(new Member(member.getServerAddress(), member.getClientAddress()));
    }
    for (MemberState member : reserveMembers) {
      members.add(new Member(member.getServerAddress(), member.getClientAddress()));
    }

    if (type != null) {
      members.add(member);
    }
    return members;
  }

  /**
   * Builds a list of active members.
   */
  Collection<Member> buildActiveMembers() {
    List<Member> members = new ArrayList<>();
    for (MemberState member : activeMembers) {
      members.add(new Member(member.getServerAddress(), member.getClientAddress()));
    }

    if (type == Type.ACTIVE) {
      members.add(member);
    }
    return members;
  }

  /**
   * Builds a list of passive members.
   */
  Collection<Member> buildPassiveMembers() {
    List<Member> members = new ArrayList<>();
    for (MemberState member : passiveMembers) {
      members.add(new Member(member.getServerAddress(), member.getClientAddress()));
    }

    if (type == Type.PASSIVE) {
      members.add(member);
    }
    return members;
  }

  /**
   * Builds a list of reserve members.
   */
  Collection<Member> buildReserveMembers() {
    List<Member> members = new ArrayList<>();
    for (MemberState member : reserveMembers) {
      members.add(new Member(member.getServerAddress(), member.getClientAddress()));
    }

    if (type == Type.RESERVE) {
      members.add(member);
    }
    return members;
  }

  /**
   * Builds a members list.
   */
  private List<MemberState> buildMembers(Collection<Member> members) {
    List<MemberState> states = new ArrayList<>(members.size());
    for (Member member : members) {
      if (!member.equals(this.member)) {
        // If the member doesn't already exist, create a new MemberState and initialize the state.
        MemberState state = membersMap.get(member.hashCode());
        if (state == null) {
          state = new MemberState(member.serverAddress());
          state.resetState(context.getLog());
        }
        states.add(state.setClientAddress(member.clientAddress()));
      }
    }
    return states;
  }

}
