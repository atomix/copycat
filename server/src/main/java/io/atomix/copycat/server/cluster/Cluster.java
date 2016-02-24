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
import io.atomix.catalyst.util.Listener;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.protocol.JoinRequest;
import io.atomix.copycat.server.protocol.LeaveRequest;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Copycat server cluster API.
 * <p>
 * This class provides the view of the Copycat cluster from the perspective of a single server. When a
 * {@link io.atomix.copycat.server.CopycatServer CopycatServer} is started, the server will form a cluster
 * with other servers. Each Copycat cluster consists of some set of {@link #members() members}, and each
 * {@link Member} represents a single server in the cluster. Users can use the {@code Cluster} to react to
 * state changes in the underlying Raft algorithm via the various listeners.
 * <p>
 * <pre>
 *   {@code
 *   server.cluster().onJoin(member -> {
 *     System.out.println(member.address() + " joined the cluster!");
 *   });
 *   }
 * </pre>
 * Membership exposed via this interface is provided from the perspective of the local server and may not
 * necessarily be consistent with cluster membership from the perspective of other nodes. The only consistent
 * membership list is on the {@link #leader() leader} node.
 * <h2>Cluster management</h2>
 * Users can use the {@code Cluster} to manage the Copycat cluster membership. Typically, servers join the
 * cluster by calling {@link CopycatServer#open()} or {@link #join()}, but in the event that a server fails
 * permanently and thus cannot remove itself, other nodes can remove arbitrary servers.
 * <p>
 * <pre>
 *   {@code
 *   server.cluster().onJoin(member -> {
 *     member.remove().thenRun(() -> System.out.println("Removed " + member.address() + " from the cluster!"));
 *   });
 *   }
 * </pre>
 * When a member is removed from the cluster, the configuration change removing the member will be replicated to all
 * the servers in the cluster and persisted to disk. Once a member has been removed, for that member to rejoin the cluster
 * it must fully restart and request to rejoin the cluster. For servers configured with a persistent
 * {@link io.atomix.copycat.server.storage.StorageLevel}, cluster configurations are stored on disk.
 * <p>
 * Additionally, members can be {@link Member#promote() promoted} and {@link Member#demote() demoted} by any
 * other member of the cluster. When a member state is changed, a cluster configuration change request is sent
 * to the cluster leader where it's logged and replicated through the Raft consensus algorithm. <em>During</em>
 * the configuration change, servers' cluster configurations will be updated. Once the configuration change is
 * complete, it will be persisted to disk on all servers and is guaranteed not to be lost even in the event of a
 * full cluster shutdown (assuming the server uses a persistent {@link io.atomix.copycat.server.storage.StorageLevel}).
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Cluster {

  /**
   * Returns the current cluster leader.
   * <p>
   * If no leader has been elected for the current {@link #term() term}, the leader will be {@code null}.
   * Once a leader is elected, the leader must be known to the local server's configuration. If the returned
   * {@link Member} is {@code null} then that does not necessarily indicate that no leader yet exists for the
   * current term, only that the local server has not learned of a valid leader for the term.
   *
   * @return The current cluster leader or {@code null} if no leader is known for the current term.
   */
  Member leader();

  /**
   * Returns the current cluster term.
   * <p>
   * The term is representative of the epoch determined by the underlying Raft consensus algorithm. The term is a monotonically
   * increasing number used by Raft to represent a point in logical time. If the cluster is persistent (i.e. all servers use a persistent
   * {@link io.atomix.copycat.server.storage.StorageLevel}), the term is guaranteed to be unique and monotonically increasing even across
   * cluster restarts. Additionally, for any given term, Raft guarantees that only a single {@link #leader() leader} can be elected.
   *
   * @return The current cluster term.
   */
  long term();

  /**
   * Registers a callback to be called when a leader is elected.
   * <p>
   * The provided {@code callback} will be called when a new leader is elected for a term. Because Raft ensures only a single leader
   * can be elected for any given term, each election callback will be called at most once per term. However, note that a leader may
   * not be elected for a term as well.
   * <pre>
   *   {@code
   *   server.cluster().onLeaderElection(member -> {
   *     System.out.println(member.address() + " elected for term " + server.cluster().term());
   *   });
   *   }
   * </pre>
   * The {@link Member} provided to the callback represents the member that was elected leader. Copycat guarantees that this member is
   * a member of the {@link Cluster}. When a leader election callback is called, the correct {@link #term()} for the leader is guaranteed
   * to have already been set. Thus, to get the term for the provided leader, simply read the cluster {@link #term()}.
   *
   * @param callback The callback to be called when a new leader is elected.
   * @return The leader election listener.
   */
  Listener<Member> onLeaderElection(Consumer<Member> callback);

  /**
   * Returns the local cluster member.
   * <p>
   * The local member is representative of the local {@link CopycatServer}. The local cluster member will always be non-null and
   * {@link io.atomix.copycat.server.cluster.Member.Status#AVAILABLE AVAILABLE} if the server is running.
   *
   * @return The local cluster member.
   */
  Member member();

  /**
   * Returns a member by ID.
   * <p>
   * The returned {@link Member} is referenced by the unique {@link Member#id()}.
   *
   * @param id The member ID.
   * @return The member or {@code null} if no member with the given {@code id} exists.
   */
  Member member(int id);

  /**
   * Returns a member by address.
   * <p>
   * The returned {@link Member} is referenced by the member's {@link Member#address()} which is synonymous
   * with the member's {@link Member#serverAddress()}.
   *
   * @param address The member server address.
   * @return The member or {@code null} if no member with the given {@link Address} exists.
   * @throws NullPointerException if {@code address} is {@code null}
   */
  Member member(Address address);

  /**
   * Returns a collection of all cluster members.
   * <p>
   * The returned members are representative of the last configuration known to the local server. Over time,
   * the cluster configuration may change. In the event of a membership change, the returned {@link Collection}
   * will not be modified, but instead a new collection will be created. Similarly, modifying the returned
   * collection will have no impact on the cluster membership.
   *
   * @return A collection of all cluster members.
   */
  Collection<Member> members();

  /**
   * Joins the cluster.
   * <p>
   * Invocations of this method will cause the local {@link CopycatServer} to join the cluster.
   * <em>This method is for advanced usage only.</em> Typically, users should use {@link CopycatServer#open()}
   * to open a new server and join the cluster in order to ensure all associated resources are property opened.
   * <p>
   * When a server joins the cluster, the server will connect to arbitrary {@link Member}s, attempting to locate
   * the cluster leader and send a {@link JoinRequest}. Once the leader has been
   * found, the leader will replicate and commit a configuration change, notifying other members of the cluster of
   * the joining server.
   * <p>
   * In order to preserve safety during configuration changes, Copycat leaders do not allow concurrent configuration
   * changes. In the event that an existing configuration change (a server joining or leaving the cluster or a
   * member being {@link Member#promote() promoted} or {@link Member#demote() demoted}) is under way, the local
   * server will retry attempts to join the cluster until successful. If the server fails to reach the leader,
   * the join will fail after a fixed number of failed attempts.
   *
   * @return A completable future to be completed once the local server has joined the cluster.
   */
  CompletableFuture<Void> join();

  /**
   * Leaves the cluster.
   * <p>
   * Invocations of this method will cause the local {@link CopycatServer} to leave the cluster.
   * <em>This method is for advanced usage only.</em> Typically, users should use {@link CopycatServer#close()}
   * to leave the cluster and close a server in order to ensure all associated resources are properly closed.
   * <p>
   * When a server leaves the cluster, the server submits a {@link LeaveRequest}
   * to the cluster leader. The leader will replicate and commit the configuration change in order to remove the
   * leaving server from the cluster and notify each member of the leaving server.
   * <p>
   * In order to preserve safety during configuration changes, Copycat leaders do not allow concurrent configuration
   * changes. In the event that an existing configuration change (a server joining or leaving the cluster or a
   * member being {@link Member#promote() promoted} or {@link Member#demote() demoted}) is under way, the local
   * server will retry attempts to leave the cluster until successful. The server will continuously attempt to
   * leave the cluster until successful.
   *
   * @return A completable future to be completed once the local server has left the cluster.
   */
  CompletableFuture<Void> leave();

  /**
   * Registers a callback to be called when a member joins the cluster.
   * <p>
   * The registered {@code callback} will be called whenever a new {@link Member} joins the cluster. Membership
   * changes are sequentially consistent, meaning each server in the cluster will see members join in the same
   * order, but different servers may see members join at different points in time. Users should not in any case
   * assume that because one server has seen a member join the cluster all servers have.
   * <p>
   * The returned {@link Listener} can be used to stop listening for servers joining the cluster.
   * <p>
   * <pre>
   *   {@code
   *   // Start listening for members joining the cluster.
   *   Listener<Member> listener = server.cluster().onJoin(member -> System.out.println(member.address() + " joined!"));
   *
   *   // Stop listening for members joining the cluster.
   *   listener.close();
   *   }
   * </pre>
   *
   * @param callback The callback to be called when a member joins the cluster.
   * @return The join listener.
   */
  Listener<Member> onJoin(Consumer<Member> callback);

  /**
   * Registers a callback to be called when a member leaves the cluster.
   * <p>
   * The registered {@code callback} will be called whenever an existing {@link Member} leaves the cluster. Membership
   * changes are sequentially consistent, meaning each server in the cluster will see members leave in the same
   * order, but different servers may see members leave at different points in time. Users should not in any case
   * assume that because one server has seen a member leave the cluster all servers have.
   * <p>
   * The returned {@link Listener} can be used to stop listening for servers leaving the cluster.
   * <p>
   * <pre>
   *   {@code
   *   // Start listening for members leaving the cluster.
   *   Listener<Member> listener = server.cluster().onLeave(member -> System.out.println(member.address() + " left!"));
   *
   *   // Stop listening for members leaving the cluster.
   *   listener.close();
   *   }
   * </pre>
   *
   * @param callback The callback to be called when a member leaves the cluster.
   * @return The leave listener.
   */
  Listener<Member> onLeave(Consumer<Member> callback);

}
