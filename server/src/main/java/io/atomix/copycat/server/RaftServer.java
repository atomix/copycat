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
package io.atomix.copycat.server;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Managed;
import io.atomix.catalyst.util.concurrent.ThreadContext;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Provides an interface for managing the lifecycle and state of a Raft server.
 * <p>
 * The lifecycle of the Raft server is managed via the {@link Managed} API methods. To start a server,
 * call {@link Managed#open()} on the server. Once the server has connected to the cluster and found
 * a leader, the returned {@link CompletableFuture} will be completed and the server will be operating.
 * <p>
 * Throughout the lifetime of a server, the server transitions between a variety of
 * {@link io.atomix.copycat.server.RaftServer.State states}. Call {@link #state()} to get the current state
 * of the server at any given point in time.
 * <p>
 * The {@link #term()} and {@link #leader()} are critical aspects of the Raft consensus algorithm. Initially,
 * when the server is started, the {@link #term()} will be initialized to {@code 0} and {@link #leader()} will
 * be {@code null}. By the time the server is fully started, both {@code term} and {@code leader} will be
 * provided. As the cluster progresses, {@link #term()} will progress monotonically, and for each term,
 * only a single {@link #leader()} will ever be elected.
 * <p>
 * Raft servers are members of a cluster of servers identified by their {@link Address}. Each server
 * must be able to locate other members of the cluster. Throughout the lifetime of a cluster, the membership
 * may change. The {@link #members()} method provides a current view of the cluster from the perspective
 * of a single server. Note that the members list may not be consistent on all nodes at any given time.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface RaftServer extends Managed<RaftServer> {

  /**
   * Raft server state types.
   * <p>
   * States represent the context of the server's internal state machine. Throughout the lifetime of a server,
   * the server will periodically transition between states based on requests, responses, and timeouts.
   *
   * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
   */
  enum State {

    /**
     * Represents the state of an inactive server.
     * <p>
     * All servers start in this state and return to this state when {@link #close() stopped}.
     */
    INACTIVE,

    /**
     * Represents the state of a server attempting to join a cluster.
     * <p>
     * When a server is {@link #open() started}, the first state to which it transitions is the join state.
     * During that period, the server will attempt to join an existing cluster.
     */
    JOIN,

    /**
     * Represents the state of a server attempting to leave a cluster.
     * <p>
     * When a server is {@link #close() stopped}, the server will first transition to the leave state and
     * remove itself from the cluster configuration during this period.
     */
    LEAVE,

    /**
     * Represents the state of a server in the process of catching up its log.
     * <p>
     * Upon successfully joining an existing cluster, the server will transition to the passive state and remain there
     * until the leader determines that the server has caught up enough to be promoted to a full member.
     */
    PASSIVE,

    /**
     * Represents the state of a server participating in normal log replication.
     * <p>
     * The follower state is a standard Raft state in which the server receives replicated log entries from the leader.
     */
    FOLLOWER,

    /**
     * Represents the state of a server attempting to become the leader.
     * <p>
     * When a server in the follower state fails to receive communication from a valid leader for some time period,
     * the follower will transition to the candidate state. During this period, the candidate requests votes from
     * each of the other servers in the cluster. If the candidate wins the election by receiving votes from a majority
     * of the cluster, it will transition to the leader state.
     */
    CANDIDATE,

    /**
     * Represents the state of a server which is actively coordinating and replicating logs with other servers.
     * <p>
     * Leaders are responsible for handling and replicating writes from clients. Note that more than one leader can
     * exist at any given time, but Raft guarantees that no two leaders will exist for the same {@link #term()}.
     */
    LEADER

  }

  /**
   * Returns the current Raft term.
   * <p>
   * The term is a monotonically increasing number that essentially acts as a logical time for the cluster. For any
   * given term, Raft guarantees that only one {@link #leader()} can be elected, but note that a leader may also
   * not yet exist for the term.
   *
   * @return The current Raft term.
   */
  long term();

  /**
   * Returns the current Raft leader.
   * <p>
   * If no leader has been elected, the leader address will be {@code null}.
   *
   * @return The current Raft leader or {@code null} if this server does not know of any leader.
   */
  Address leader();

  /**
   * Returns a collection of current cluster members.
   * <p>
   * The current members list includes members in all states, including non-voting states. Additionally, because
   * the membership set can change over time, the set of members on one server may not exactly reflect the
   * set of members on another server at any given point in time.
   *
   * @return A collection of current Raft cluster members.
   */
  Collection<Address> members();

  /**
   * Returns the Raft server state.
   * <p>
   * The initial state of a Raft server is {@link State#INACTIVE}. Once the server is {@link #open() started} and
   * until it is explicitly shutdown, the server will be in one of the active states - {@link State#PASSIVE},
   * {@link State#FOLLOWER}, {@link State#CANDIDATE}, or {@link State#LEADER}.
   *
   * @return The Raft server state.
   */
  State state();

  /**
   * Returns the server execution context.
   * <p>
   * The thread context is the event loop that this server uses to communicate other Raft servers.
   * Implementations must guarantee that all asynchronous {@link java.util.concurrent.CompletableFuture} callbacks are
   * executed on a single thread via the returned {@link io.atomix.catalyst.util.concurrent.ThreadContext}.
   * <p>
   * The {@link io.atomix.catalyst.util.concurrent.ThreadContext} can also be used to access the Raft server's internal
   * {@link io.atomix.catalyst.serializer.Serializer serializer} via {@link ThreadContext#serializer()}. Catalyst serializers
   * are not thread safe, so to use the context serializer, users should clone it:
   * <pre>
   *   {@code
   *   Serializer serializer = server.threadContext().serializer().clone();
   *   Buffer buffer = serializer.writeObject(myObject).flip();
   *   }
   * </pre>
   *
   * @return The server thread context.
   */
  ThreadContext context();

  /**
   * Starts the Raft server asynchronously.
   * <p>
   * When the server is started, the server will attempt to search for an existing cluster by contacting all of
   * the members in the provided members list. If no existing cluster is found, the server will immediately transition
   * to the {@link State#FOLLOWER} state and continue normal Raft protocol operations. If a cluster is found, the server
   * will attempt to join the cluster. Once the server has joined or started a cluster and a leader has been found,
   * the returned {@link CompletableFuture} will be completed.
   *
   * @return A completable future to be completed once the server has joined the cluster and a leader has been found.
   */
  @Override
  CompletableFuture<RaftServer> open();

  /**
   * Returns a boolean value indicating whether the server is running.
   * <p>
   * Once {@link #open()} is called and the returned {@link CompletableFuture} is completed (meaning this server found
   * a cluster leader), this method will return {@code true} until {@link #close() closed}.
   *
   * @return Indicates whether the server is running.
   */
  @Override
  boolean isOpen();

}
