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
package io.atomix.copycat.server.cluster;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.concurrent.Listener;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Represents a member of a Copycat cluster.
 * <p>
 * This interface provides metadata and operations related to a specific member of a Copycat cluster.
 * Each server in a {@link Cluster} has a view of the cluster state and can reference and operate on
 * specific members of the cluster via this API.
 * <p>
 * Each member in the cluster has an associated {@link Member.Type} and {@link Member.Status}. The
 * {@link Member.Type} is indicative of the manner in which the member interacts with other members of
 * the cluster. The {@link Member.Status} is indicative of the leader's ability to communicate with the
 * member. Additionally, each member is identified by an {@link #address() address} and unique {@link #id() ID}
 * which is generated from the {@link Address} hash code. The member's {@link Address} represents the
 * address through which the server communicates with other servers and not through which clients
 * communicate with the member (which may be a different {@link Address}).
 * <p>
 * Users can listen for {@link Member.Type} and {@link Member.Status} changes via the
 * {@link #onTypeChange(Consumer)} and {@link #onStatusChange(Consumer)} methods respectively. Member types
 * can be modified by virtually any member of the cluster via the {@link #promote()} and {@link #demote()}
 * methods. This allows servers to modify the way dead nodes interact with the cluster and modify the
 * Raft quorum size without requiring the member being modified to be available. The member status is
 * controlled only by the cluster {@link Cluster#leader() leader}. When the leader fails to contact a
 * member for a few rounds of heartbeats, the leader will commit a configuration change marking that
 * member as {@link Member.Status#UNAVAILABLE}. Once the member can be reached again, the leader will
 * update its status back to {@link Member.Status#AVAILABLE}.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Member {

  /**
   * Indicates how the member participates in voting and replication.
   * <p>
   * The member type defines how a member interacts with the other members of the cluster and, more
   * importantly, how the cluster {@link Cluster#leader() leader} interacts with the member server.
   * Members can be {@link #promote() promoted} and {@link #demote() demoted} to alter member states.
   * See the specific member types for descriptions of their implications on the cluster.
   */
  enum Type {

    /**
     * Represents an inactive member.
     * <p>
     * The {@code INACTIVE} member type represents a member which does not participate in any communication
     * and is not an active member of the cluster. This is typically the state of a member prior to joining
     * or after leaving a cluster.
     */
    INACTIVE,

    /**
     * Represents a member which does not participate in replication.
     * <p>
     * The {@code RESERVE} member type is representative of a member that does not participate in any
     * replication of state but only maintains contact with the cluster leader and is an active member
     * of the {@link Cluster}. Typically, reserve members act as standby nodes which can be
     * {@link #promote() promoted} to a {@link #PASSIVE} or {@link #ACTIVE} role when needed.
     */
    RESERVE,

    /**
     * Represents a member which participates in asynchronous replication but does not vote in elections
     * or otherwise participate in the Raft consensus algorithm.
     * <p>
     * The {@code PASSIVE} member type is representative of a member that receives state changes from
     * follower nodes asynchronously. As state changes are committed via the {@link #ACTIVE} Raft nodes,
     * committed state changes are asynchronously replicated by followers to passive members. This allows
     * passive members to maintain nearly up-to-date state with minimal impact on the performance of the
     * Raft algorithm itself, and allows passive members to be quickly promoted to {@link #ACTIVE} voting
     * members if necessary.
     */
    PASSIVE,

    /**
     * Represents a full voting member of the Raft cluster which participates fully in leader election
     * and replication algorithms.
     * <p>
     * The {@code ACTIVE} member type represents a full voting member of the Raft cluster. Active members
     * participate in the Raft leader election and replication algorithms and can themselves be elected
     * leaders.
     */
    ACTIVE,

  }

  /**
   * Indicates the availability of a member from the perspective of the cluster {@link Cluster#leader() leader}.
   * <p>
   * Member statuses are manged by the cluster {@link Cluster#leader() leader}. For each {@link Member} of a
   * {@link Cluster}, the leader periodically sends a heartbeat to the member to determine its availability.
   * In the event that the leader cannot contact a member for more than a few heartbeats, the leader will
   * set the member's availability status to {@link #UNAVAILABLE}. Once the leader reestablishes communication
   * with the member, it will reset its status back to {@link #AVAILABLE}.
   */
  enum Status {

    /**
     * Indicates that a member is reachable by the leader.
     * <p>
     * Availability is determined by the leader's ability to successfully send heartbeats to the member. If the
     * last heartbeat attempt to the member was successful, its status will be available. For members whose status
     * is {@link #UNAVAILABLE}, once the leader is able to heartbeat the member its status will be reset to available.
     */
    AVAILABLE,

    /**
     * Indicates that a member is unreachable by the leader.
     * <p>
     * Availability is determined by the leader's ability to successfully send heartbeats to the member. If the
     * leader repeatedly fails to heartbeat a member, the leader will eventually commit a configuration change setting
     * the member's status to unavailable. Once the leader is able to contact the member again, its status will be
     * reset to {@link #AVAILABLE}.
     */
    UNAVAILABLE,

  }

  /**
   * Returns the member ID.
   * <p>
   * The member ID is calculated as the hash of the member's server {@link Address} and is therefore
   * equivalent to the return value of {@link Address#hashCode()}. The member ID simply provides a more
   * compact identifier to send on the wire when communicating between servers.
   *
   * @return The member ID.
   */
  int id();

  /**
   * Returns the member server address.
   * <p>
   * This is the primary address through which servers in the cluster identify and communicate with one another.
   * The server address is guaranteed to be unique to each member in the cluster.
   *
   * @return The member server address. This is the same address as {@link #serverAddress()}.
   */
  Address address();

  /**
   * Returns the member client address.
   * <p>
   * THis is the address through which clients communicate with the member. Depending on the member's configuration,
   * the client address may or may not be the same as the {@link #serverAddress()}, but it will always be non-null.
   *
   * @return The address to which the member binds for client communication.
   */
  Address clientAddress();

  /**
   * Returns the member server address.
   * <p>
   * The server address is the primary address through which servers in the cluster communicate with one another.
   * The server address is guaranteed to be unique to each member in the cluster.
   *
   * @return The address to which the member binds for server communication.
   */
  Address serverAddress();

  /**
   * Returns the member type.
   * <p>
   * The member type is indicative of the member's level of participation in the Raft consensus algorithm and
   * asynchronous replication within the cluster. Member types may change throughout the lifetime of the cluster.
   * Types can be changed by {@link #promote(Type) promoting} or {@link #demote(Type) demoting} the member. Member
   * types for a given member are guaranteed to change in the same order on all nodes, but the type of a member
   * may be different from the perspective of different nodes at any given time.
   *
   * @return The member type.
   */
  Type type();

  /**
   * Registers a callback to be called when the member's type changes.
   * <p>
   * The type change callback will be called when the local server receives notification of the change in type
   * to this member. Type changes may occur at different times from the perspective of different servers but are
   * guaranteed to occur in the same order on all servers.
   *
   * @param callback The callback to be called when the member's type changes.
   * @return The member type change listener.
   */
  Listener<Type> onTypeChange(Consumer<Type> callback);

  /**
   * Returns the member status.
   * <p>
   * The status is indicative of the leader's ability to communicate with this member. If this member is a local
   * member, the member's status will be {@link Status#AVAILABLE} while the server is alive and will not change
   * regardless of the leader's ability to communicate with the local member. Similarly, if the local server is
   * partitioned from the leader then changes in statuses seen on other nodes may not be visible to this node.
   * Status changes are guaranteed to occur in the same order on all nodes but without a real-time constraint.
   *
   * @return The member status.
   */
  Status status();

  /**
   * Returns the time at which the member was updated.
   * <p>
   * The member update time is not guaranteed to be consistent across servers or consistent across server
   * restarts. The update time is guaranteed to be monotonically increasing.
   *
   * @return The time at which the member was updated.
   */
  Instant updated();

  /**
   * Registers a callback to be called when the member's status changes.
   * <p>
   * The status change callback will be called when the local server receives notification of the change in status
   * to this member. Status changes may occur at different times from the perspective of different servers but are
   * guaranteed to occur in the same order on all servers.
   *
   * @param callback The callback to be called when the member's status changes.
   * @return The member status change listener.
   */
  Listener<Status> onStatusChange(Consumer<Status> callback);

  /**
   * Promotes the member to the next highest type.
   * <p>
   * If the member is promoted to {@link Type#ACTIVE} the Raft quorum size will increase.
   *
   * @return A completable future to be completed once the member has been promoted.
   */
  CompletableFuture<Void> promote();

  /**
   * Promotes the member to the given type.
   * <p>
   * If the member is promoted to {@link Type#ACTIVE} the Raft quorum size will increase.
   *
   * @param type The type to which to promote the member.
   * @return A completable future to be completed once the member has been promoted.
   */
  CompletableFuture<Void> promote(Member.Type type);

  /**
   * Demotes the member to the next lowest type.
   * <p>
   * If the member is an {@link Type#ACTIVE} member then demoting it will impact the Raft quorum size.
   *
   * @return A completable future to be completed once the member has been demoted.
   */
  CompletableFuture<Void> demote();

  /**
   * Demotes the member to the given type.
   * <p>
   * If the member is an {@link Type#ACTIVE} member then demoting it will impact the Raft quorum size.
   *
   * @param type The type to which to demote the member.
   * @return A completable future to be completed once the member has been demoted.
   */
  CompletableFuture<Void> demote(Member.Type type);

  /**
   * Removes the member from the configuration.
   * <p>
   * If the member is a part of the current Raft quorum (is an {@link Type#ACTIVE} member) then the
   * quorum will be impacted by removing the member.
   *
   * @return A completable future to be completed once the member has been removed from the configuration.
   */
  CompletableFuture<Void> remove();

}
