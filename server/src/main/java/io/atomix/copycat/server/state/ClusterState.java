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

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.catalyst.util.concurrent.Scheduled;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.cluster.Cluster;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.request.ReconfigureRequest;
import io.atomix.copycat.server.request.ConfigurationRequest;
import io.atomix.copycat.server.request.JoinRequest;
import io.atomix.copycat.server.request.LeaveRequest;
import io.atomix.copycat.server.response.ConfigurationResponse;
import io.atomix.copycat.server.response.JoinResponse;
import io.atomix.copycat.server.storage.system.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Cluster state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class ClusterState implements Cluster, AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClusterState.class);
  private static final int MAX_JOIN_ATTEMPTS = 3;
  private final ServerContext context;
  private final ServerMember member;
  private final Member.Type initialType;
  private long index = -1;
  private final Map<Integer, MemberState> membersMap = new ConcurrentHashMap<>();
  private final Map<Address, MemberState> addressMap = new ConcurrentHashMap<>();
  private final List<MemberState> members = new ArrayList<>();
  private List<MemberState> assignedPassiveMembers = new ArrayList<>();
  private final Map<Member.Type, List<MemberState>> memberTypes = new HashMap<>();
  private Scheduled joinTimeout;
  private volatile CompletableFuture<Void> joinFuture;
  private Scheduled leaveTimeout;
  private final Listeners<Member> joinListeners = new Listeners<>();
  private final Listeners<Member> leaveListeners = new Listeners<>();

  ClusterState(ServerContext context, ServerMember member, Member.Type initialType) {
    this.context = Assert.notNull(context, "context");
    this.member = Assert.notNull(member, "member");
    this.initialType = Assert.notNull(initialType, "initialType");
  }

  /**
   * Returns the parent context.
   *
   * @return The parent context.
   */
  ServerContext getContext() {
    return context;
  }

  @Override
  public Member leader() {
    return context.getLeader();
  }

  @Override
  public long term() {
    return context.getTerm();
  }

  @Override
  public Listener<Member> onLeaderElection(Consumer<Member> callback) {
    return context.onLeaderElection(callback);
  }

  @Override
  public Member member() {
    return member;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Collection<Member> members() {
    // Add all members to a list. The "members" field is only remote members, so we must separately
    // add the local member to the list if necessary.
    List<Member> members = new ArrayList<>(this.members.size() + 1);
    for (MemberState member : this.members) {
      members.add(member.getMember());
    }

    // If the local member type is null, that indicates it's not a member of the current configuration.
    if (member.type() != Member.Type.INACTIVE) {
      members.add(member);
    }
    return members;
  }

  @Override
  public ServerMember member(int id) {
    if (member.id() == id) {
      return member;
    }
    return getRemoteMember(id);
  }

  @Override
  public Member member(Address address) {
    if (member.address().equals(address)) {
      return member;
    }

    MemberState member = addressMap.get(address);
    return member != null ? member.getMember() : null;
  }

  @Override
  public Listener<Member> onJoin(Consumer<Member> callback) {
    return joinListeners.add(callback);
  }

  @Override
  public Listener<Member> onLeave(Consumer<Member> callback) {
    return leaveListeners.add(callback);
  }

  /**
   * Returns the remote quorum count.
   *
   * @return The remote quorum count.
   */
  int getQuorum() {
    return (int) Math.floor((getActiveMemberStates().size() + 1) / 2.0) + 1;
  }

  /**
   * Returns the cluster state index.
   *
   * @return The cluster state index.
   */
  long getVersion() {
    return index;
  }

  /**
   * Returns a member by ID.
   *
   * @param id The member ID.
   * @return The member.
   */
  public ServerMember getRemoteMember(int id) {
    MemberState member = membersMap.get(id);
    return member != null ? member.getMember() : null;
  }

  /**
   * Returns a list of all member states.
   *
   * @return A list of all member states.
   */
  public List<MemberState> getRemoteMemberStates() {
    return members;
  }

  /**
   * Returns a list of member states for the given type.
   *
   * @param type The member type.
   * @return A list of member states for the given type.
   */
  public List<MemberState> getRemoteMemberStates(Member.Type type) {
    List<MemberState> members = memberTypes.get(type);
    return members != null ? members : Collections.EMPTY_LIST;
  }

  /**
   * Returns a list of active members.
   *
   * @return A list of active members.
   */
  List<MemberState> getActiveMemberStates() {
    return getRemoteMemberStates(Member.Type.ACTIVE);
  }

  /**
   * Returns a list of active members.
   *
   * @param comparator A comparator with which to sort the members list.
   * @return The sorted members list.
   */
  List<MemberState> getActiveMemberStates(Comparator<MemberState> comparator) {
    List<MemberState> activeMembers = getActiveMemberStates();
    Collections.sort(activeMembers, comparator);
    return activeMembers;
  }

  /**
   * Returns a list of promotable members.
   *
   * @return A list of promotable members.
   */
  List<MemberState> getPromotableMemberStates() {
    return getRemoteMemberStates(Member.Type.PROMOTABLE);
  }

  /**
   * Returns a list of promotable members.
   *
   * @param comparator A comparator with which to sort the members list.
   * @return The sorted members list.
   */
  List<MemberState> getPromotableMemberStates(Comparator<MemberState> comparator) {
    List<MemberState> promotableMembers = getPromotableMemberStates();
    Collections.sort(promotableMembers, comparator);
    return promotableMembers;
  }

  /**
   * Returns a list of passive members.
   *
   * @return A list of passive members.
   */
  List<MemberState> getPassiveMemberStates() {
    return getRemoteMemberStates(Member.Type.PASSIVE);
  }

  /**
   * Returns a list of passive members.
   *
   * @param comparator A comparator with which to sort the members list.
   * @return The sorted members list.
   */
  List<MemberState> getPassiveMemberStates(Comparator<MemberState> comparator) {
    List<MemberState> passiveMembers = getPassiveMemberStates();
    Collections.sort(passiveMembers, comparator);
    return passiveMembers;
  }

  /**
   * Returns a list of reserve members.
   *
   * @return A list of reserve members.
   */
  List<MemberState> getReserveMemberStates() {
    return getRemoteMemberStates(Member.Type.RESERVE);
  }

  /**
   * Returns a list of reserve members.
   *
   * @param comparator A comparator with which to sort the members list.
   * @return The sorted members list.
   */
  List<MemberState> getReserveMemberStates(Comparator<MemberState> comparator) {
    List<MemberState> reserveMembers = getReserveMemberStates();
    Collections.sort(reserveMembers, comparator);
    return reserveMembers;
  }

  /**
   * Returns a list of assigned passive member states.
   *
   * @return A list of assigned passive member states.
   */
  List<MemberState> getAssignedPassiveMemberStates() {
    return assignedPassiveMembers;
  }

  @Override
  public CompletableFuture<Void> join() {
    if (joinFuture != null)
      return joinFuture;

    joinFuture = new CompletableFuture<>();

    // If the server type is defined, that indicates it's a member of the current configuration and
    // doesn't need to be added to the configuration. Immediately transition to the appropriate state.
    // Note that we don't complete the join future when transitioning to a valid state since we need
    // to ensure that the server's configuration has been updated in the cluster before completing the join.
    switch (member.type()) {
      case INACTIVE:
        join(getActiveMemberStates().iterator(), 1);
        break;
      case RESERVE:
        context.transition(CopycatServer.State.RESERVE);
        break;
      case PASSIVE:
      case PROMOTABLE:
        context.transition(CopycatServer.State.PASSIVE);
        break;
      case ACTIVE:
        context.transition(CopycatServer.State.FOLLOWER);
        break;
    }

    return joinFuture.whenComplete((result, error) -> joinFuture = null);
  }

  /**
   * Recursively attempts to join the cluster.
   */
  private void join(Iterator<MemberState> iterator, int attempts) {
    if (iterator.hasNext()) {
      MemberState member = iterator.next();
      LOGGER.debug("{} - Attempting to join via {}", member().address(), member.getMember().serverAddress());

      context.getConnections().getConnection(member.getMember().serverAddress()).thenCompose(connection -> {
        JoinRequest request = JoinRequest.builder()
          .withMember(new ServerMember(initialType, member().serverAddress(), member().clientAddress()))
          .build();
        return connection.<JoinRequest, JoinResponse>send(request);
      }).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status() == Response.Status.OK) {
            LOGGER.info("{} - Successfully joined via {}", member().address(), member.getMember().serverAddress());

            // Configure the cluster with the join response.
            configure(response.index(), response.members());

            // Cancel the join timer.
            cancelJoinTimer();

            // If the local member type is null, that indicates it's not a part of the configuration.
            Member.Type type = member().type();
            if (type == null) {
              joinFuture.completeExceptionally(new IllegalStateException("not a member of the cluster"));
            } else if (type == Member.Type.ACTIVE) {
              context.transition(CopycatServer.State.FOLLOWER);
              joinFuture.complete(null);
            } else if (type == Member.Type.PASSIVE || type == Member.Type.PROMOTABLE) {
              context.transition(CopycatServer.State.PASSIVE);
              joinFuture.complete(null);
            } else if (type == Member.Type.RESERVE) {
              context.transition(CopycatServer.State.RESERVE);
              joinFuture.complete(null);
            } else {
              joinFuture.completeExceptionally(new IllegalStateException("unknown member type: " + type));
            }
          } else if (response.error() == null) {
            // If the response error is null, that indicates that no error occurred but the leader was
            // in a state that was incapable of handling the join request. Attempt to join the leader
            // again after an election timeout.
            LOGGER.debug("{} - Failed to join {}", member().address(), member.getMember().address());
            cancelJoinTimer();
            joinTimeout = context.getThreadContext().schedule(context.getElectionTimeout(), () -> {
              join(getActiveMemberStates().iterator(), attempts);
            });
          } else {
            // If the response error was non-null, attempt to join via the next server in the members list.
            LOGGER.debug("{} - Failed to join {}", member().address(), member.getMember().address());
            join(iterator, attempts);
          }
        } else {
          LOGGER.debug("{} - Failed to join {}", member().address(), member.getMember().address());
          join(iterator, attempts);
        }
      });
    }
    // If the maximum number of join attempts has been reached, fail the join.
    else if (attempts >= MAX_JOIN_ATTEMPTS) {
      joinFuture.completeExceptionally(new IllegalStateException("failed to join the cluster"));
    }
    // If join attempts remain, schedule another attempt after two election timeouts. This allows enough time
    // for servers to potentially timeout and elect a leader.
    else {
      cancelJoinTimer();
      joinTimeout = context.getThreadContext().schedule(context.getElectionTimeout().multipliedBy(2), () -> {
        join(getActiveMemberStates().iterator(), attempts + 1);
      });
    }
  }

  /**
   * Identifies this server to the leader.
   */
  CompletableFuture<Void> identify() {
    Member leader = context.getLeader();
    if (joinFuture != null && leader != null) {
      if (context.getLeader().equals(member())) {
        if (context.getState() == CopycatServer.State.LEADER && !((LeaderState) context.getAbstractState()).configuring()) {
          joinFuture.complete(null);
        } else {
          cancelJoinTimer();
          joinTimeout = context.getThreadContext().schedule(context.getElectionTimeout().multipliedBy(2), this::identify);
        }
      } else {
        LOGGER.debug("{} - Sending server identification to {}", member().address(), leader.address());
        context.getConnections().getConnection(leader.serverAddress()).thenCompose(connection -> {
          ReconfigureRequest request = ReconfigureRequest.builder()
            .withIndex(getVersion())
            .withMember(member())
            .build();
          return connection.<ConfigurationRequest, ConfigurationResponse>send(request);
        }).whenComplete((response, error) -> {
          if (error == null) {
            if (response.status() == Response.Status.OK) {
              cancelJoinTimer();
              if (joinFuture != null)
                joinFuture.complete(null);
            } else if (response.error() == null) {
              cancelJoinTimer();
              joinTimeout = context.getThreadContext().schedule(context.getElectionTimeout().multipliedBy(2), this::identify);
            }
          }
        });
      }
    }
    return joinFuture;
  }

  /**
   * Cancels the join timeout.
   */
  private void cancelJoinTimer() {
    if (joinTimeout != null) {
      LOGGER.debug("{} - Cancelling join timeout", member().address());
      joinTimeout.cancel();
      joinTimeout = null;
    }
  }

  /**
   * Leaves the cluster.
   */
  public CompletableFuture<Void> leave() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    context.getThreadContext().execute(() -> {
      if (getActiveMemberStates().isEmpty()) {
        LOGGER.debug("{} - Single member cluster. Transitioning directly to inactive.", member().address());
        context.transition(CopycatServer.State.INACTIVE);
        future.complete(null);
      } else {
        leave(future);
      }
    });
    return future;
  }

  /**
   * Attempts to leave the cluster.
   */
  private void leave(CompletableFuture<Void> future) {
    // Set a timer to retry the attempt to leave the cluster.
    leaveTimeout = context.getThreadContext().schedule(context.getElectionTimeout(), () -> {
      leave(future);
    });

    // Attempt to leave the cluster by submitting a LeaveRequest directly to the server state.
    // Non-leader states should forward the request to the leader if there is one. Leader states
    // will log, replicate, and commit the reconfiguration.
    context.getAbstractState().leave(LeaveRequest.builder()
      .withMember(member())
      .build()).whenComplete((response, error) -> {
      if (error == null && response.status() == Response.Status.OK) {
        cancelLeaveTimer();
        configure(response.index(), response.members());
        context.transition(CopycatServer.State.INACTIVE);
        future.complete(null);
      }
    });
  }

  /**
   * Cancels the leave timeout.
   */
  private void cancelLeaveTimer() {
    if (leaveTimeout != null) {
      LOGGER.debug("{} - Cancelling leave timeout", member().address());
      leaveTimeout.cancel();
      leaveTimeout = null;
    }
  }

  /**
   * Configures the cluster state.
   *
   * @param index The cluster state index.
   * @param members The cluster members.
   * @return The cluster state.
   */
  ClusterState configure(long index, Collection<Member> members) {
    if (index <= this.index)
      return this;

    // If the configuration index is less than the currently configured index, ignore it.
    // Configurations can be persisted and applying old configurations can revert newer configurations.
    if (index <= this.index)
      return this;

    // Iterate through members in the new configuration, add any missing members, and update existing members.
    for (Member member : members) {
      if (member.equals(this.member)) {
        this.member.update(member.type()).update(member.clientAddress());
      } else {
        // If the member state doesn't already exist, create it.
        MemberState state = membersMap.get(member.id());
        if (state == null) {
          state = new MemberState(new ServerMember(member.type(), member.serverAddress(), member.clientAddress()), this);
          state.resetState(context.getLog());
          this.members.add(state);
          membersMap.put(member.id(), state);
          addressMap.put(member.address(), state);
          joinListeners.accept(state.getMember());
        }

        // If the member type has changed, update the member type and reset its state.
        state.getMember().update(member.clientAddress());
        if (state.getMember().type() != member.type()) {
          state.getMember().update(member.type());
          state.resetState(context.getLog());
        }

        // If the member status has changed, update the local member status.
        if (state.getMember().status() != member.status()) {
          state.getMember().update(member.status());
        }

        // Update the optimized member collections according to the member type.
        for (List<MemberState> memberType : memberTypes.values()) {
          memberType.remove(state);
        }

        List<MemberState> memberType = memberTypes.get(member.type());
        if (memberType == null) {
          memberType = new ArrayList<>();
          memberTypes.put(member.type(), memberType);
        }
        memberType.add(state);
      }
    }

    // If the local member is not part of the configuration, set its type to null.
    if (!members.contains(this.member)) {
      this.member.update(Member.Type.INACTIVE);
    }

    // Iterate through configured members and remove any that no longer exist in the configuration.
    Iterator<MemberState> iterator = this.members.iterator();
    while (iterator.hasNext()) {
      MemberState member = iterator.next();
      if (!members.contains(member.getMember())) {
        iterator.remove();
        for (List<MemberState> memberType : memberTypes.values()) {
          memberType.remove(member);
        }
        membersMap.remove(member.getMember().id());
        addressMap.remove(member.getMember().address());
        leaveListeners.accept(member.getMember());
      }
    }

    this.index = index;

    // Store the configuration to ensure it can be easily loaded on server restart.
    context.getMetaStore().storeConfiguration(new Configuration(index, members));

    // Reassign members based on availability.
    reassign();

    return this;
  }

  /**
   * Rebuilds assigned member states.
   */
  private void reassign() {
    if (member.type() == Member.Type.ACTIVE && !member.equals(context.getLeader())) {
      // Calculate this server's index within the collection of active members, excluding the leader.
      // This is done in a deterministic way by sorting the list of active members by ID.
      int index = 1;
      for (MemberState member : getActiveMemberStates((m1, m2) -> m1.getMember().id() - m2.getMember().id())) {
        if (!member.getMember().equals(context.getLeader())) {
          if (this.member.id() < member.getMember().id()) {
            index++;
          } else {
            break;
          }
        }
      }

      // Intersect the active members list with a sorted list of passive members to get assignments.
      List<MemberState> sortedPassiveMembers = getPassiveMemberStates((m1, m2) -> m1.getMember().id() - m2.getMember().id());
      assignedPassiveMembers = assignMembers(index, sortedPassiveMembers);
    } else {
      assignedPassiveMembers = new ArrayList<>(0);
    }
  }

  /**
   * Assigns members using consistent hashing.
   */
  private List<MemberState> assignMembers(int index, List<MemberState> sortedMembers) {
    List<MemberState> members = new ArrayList<>(sortedMembers.size());
    for (int i = 0; i < sortedMembers.size(); i++) {
      if ((i + 1) % index == 0) {
        members.add(sortedMembers.get(i));
      }
    }
    return members;
  }

  @Override
  public void close() {
    for (MemberState member : members) {
      member.getMember().close();
    }
    cancelJoinTimer();
  }

}
