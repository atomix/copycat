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
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.catalyst.util.concurrent.Scheduled;
import io.atomix.catalyst.util.concurrent.SingleThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.request.*;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.RaftServer;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.request.*;
import io.atomix.copycat.server.response.JoinResponse;
import io.atomix.copycat.server.storage.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Raft server state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ServerState {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerState.class);
  private final Listeners<CopycatServer.State> stateChangeListeners = new Listeners<>();
  private final Listeners<Address> electionListeners = new Listeners<>();
  private ThreadContext threadContext;
  private final StateMachine userStateMachine;
  private final ClusterState cluster;
  private final Log log;
  private final ServerStateMachine stateMachine;
  private final ConnectionManager connections;
  private AbstractState state = new InactiveState(this);
  private Duration electionTimeout = Duration.ofMillis(500);
  private Duration sessionTimeout = Duration.ofMillis(5000);
  private Duration heartbeatInterval = Duration.ofMillis(150);
  private Scheduled joinTimer;
  private Scheduled leaveTimer;
  private int leader;
  private long term;
  private int lastVotedFor;
  private long commitIndex;

  @SuppressWarnings("unchecked")
  ServerState(Member member, Collection<Address> members, Log log, StateMachine stateMachine, ConnectionManager connections, ThreadContext threadContext) {
    Set<Member> activeMembers = members.stream().map(a -> new Member(RaftMemberType.ACTIVE, a, null)).collect(Collectors.toSet());
    activeMembers.add(member);
    this.cluster = new ClusterState(this, member);
    this.log = Assert.notNull(log, "log");
    this.threadContext = Assert.notNull(threadContext, "threadContext");
    this.connections = Assert.notNull(connections, "connections");
    this.userStateMachine = Assert.notNull(stateMachine, "stateMachine");

    // Create a state machine executor and configure the state machine.
    ThreadContext stateContext = new SingleThreadContext("copycat-server-" + member.serverAddress() + "-state-%d", threadContext.serializer().clone());
    this.stateMachine = new ServerStateMachine(userStateMachine, new ServerStateMachineContext(connections, new ServerSessionManager()), log::clean, stateContext);

    cluster.configure(0, activeMembers);
  }

  /**
   * Registers a state change listener.
   *
   * @param listener The state change listener.
   * @return The listener context.
   */
  public Listener<CopycatServer.State> onStateChange(Consumer<CopycatServer.State> listener) {
    return stateChangeListeners.add(listener);
  }

  /**
   * Registers a leader election listener.
   *
   * @param listener The leader election listener.
   * @return The listener context.
   */
  public Listener<Address> onLeaderElection(Consumer<Address> listener) {
    return electionListeners.add(listener);
  }

  /**
   * Returns the execution context.
   *
   * @return The execution context.
   */
  public ThreadContext getThreadContext() {
    return threadContext;
  }

  /**
   * Returns the context connection manager.
   *
   * @return The context connection manager.
   */
  ConnectionManager getConnections() {
    return connections;
  }

  /**
   * Sets the election timeout.
   *
   * @param electionTimeout The election timeout.
   * @return The Raft context.
   */
  public ServerState setElectionTimeout(Duration electionTimeout) {
    this.electionTimeout = electionTimeout;
    return this;
  }

  /**
   * Returns the election timeout.
   *
   * @return The election timeout.
   */
  public Duration getElectionTimeout() {
    return electionTimeout;
  }

  /**
   * Sets the heartbeat interval.
   *
   * @param heartbeatInterval The Raft heartbeat interval.
   * @return The Raft context.
   */
  public ServerState setHeartbeatInterval(Duration heartbeatInterval) {
    this.heartbeatInterval = heartbeatInterval;
    return this;
  }

  /**
   * Returns the heartbeat interval.
   *
   * @return The heartbeat interval.
   */
  public Duration getHeartbeatInterval() {
    return heartbeatInterval;
  }

  /**
   * Returns the session timeout.
   *
   * @return The session timeout.
   */
  public Duration getSessionTimeout() {
    return sessionTimeout;
  }

  /**
   * Sets the session timeout.
   *
   * @param sessionTimeout The session timeout.
   * @return The Raft state machine.
   */
  public ServerState setSessionTimeout(Duration sessionTimeout) {
    this.sessionTimeout = sessionTimeout;
    return this;
  }

  /**
   * Sets the state leader.
   *
   * @param leader The state leader.
   * @return The Raft context.
   */
  ServerState setLeader(int leader) {
    if (this.leader == 0) {
      if (leader != 0) {
        Member member = cluster.getMember(leader);
        Assert.state(member != null, "unknown leader: ", leader);
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.info("{} - Found leader {}", cluster.getMember().serverAddress(), member.serverAddress());
        electionListeners.forEach(l -> l.accept(member.serverAddress()));
      }
    } else if (leader != 0) {
      if (this.leader != leader) {
        Member member = cluster.getMember(leader);
        Assert.state(member != null, "unknown leader: ", leader);
        this.leader = leader;
        this.lastVotedFor = 0;
        LOGGER.info("{} - Found leader {}", cluster.getMember().serverAddress(), member.serverAddress());
        electionListeners.forEach(l -> l.accept(member.serverAddress()));
      }
    } else {
      this.leader = 0;
    }
    return this;
  }

  /**
   * Returns the cluster state.
   *
   * @return The cluster state.
   */
  ClusterState getCluster() {
    return cluster;
  }

  /**
   * Returns a collection of current members.
   *
   * @return A collection of current members.
   */
  public Collection<Address> getMembers() {
    return cluster.getMembers().stream().map(Member::serverAddress).collect(Collectors.toSet());
  }

  /**
   * Returns the state leader.
   *
   * @return The state leader.
   */
  public Address getLeader() {
    if (leader == 0) {
      return null;
    } else if (leader == cluster.getMember().id()) {
      return cluster.getMember().serverAddress();
    }

    Member member = cluster.getRemoteMember(leader);
    return member != null ? member.serverAddress() : null;
  }

  /**
   * Sets the state term.
   *
   * @param term The state term.
   * @return The Raft context.
   */
  ServerState setTerm(long term) {
    if (term > this.term) {
      this.term = term;
      this.leader = 0;
      this.lastVotedFor = 0;
      LOGGER.debug("{} - Set term {}", cluster.getMember().serverAddress(), term);
    }
    return this;
  }

  /**
   * Returns the state term.
   *
   * @return The state term.
   */
  public long getTerm() {
    return term;
  }

  /**
   * Sets the state last voted for candidate.
   *
   * @param candidate The candidate that was voted for.
   * @return The Raft context.
   */
  ServerState setLastVotedFor(int candidate) {
    // If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
    Assert.stateNot(lastVotedFor != 0 && candidate != 0l, "Already voted for another candidate");
    Assert.stateNot (leader != 0 && candidate != 0, "Cannot cast vote - leader already exists");
    Member member = cluster.getMember(candidate);
    Assert.state(member != null, "unknown candidate: %d", candidate);
    this.lastVotedFor = candidate;

    if (candidate != 0) {
      LOGGER.debug("{} - Voted for {}", cluster.getMember().serverAddress(), member.serverAddress());
    } else {
      LOGGER.debug("{} - Reset last voted for", cluster.getMember().serverAddress());
    }
    return this;
  }

  /**
   * Returns the state last voted for candidate.
   *
   * @return The state last voted for candidate.
   */
  public int getLastVotedFor() {
    return lastVotedFor;
  }

  /**
   * Sets the commit index.
   *
   * @param commitIndex The commit index.
   * @return The Raft context.
   */
  ServerState setCommitIndex(long commitIndex) {
    Assert.argNot(commitIndex < 0, "commit index must be positive");
    Assert.argNot(commitIndex < this.commitIndex, "cannot decrease commit index");
    this.commitIndex = commitIndex;
    return this;
  }

  /**
   * Returns the commit index.
   *
   * @return The commit index.
   */
  public long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Returns the server state machine.
   *
   * @return The server state machine.
   */
  ServerStateMachine getStateMachine() {
    return stateMachine;
  }

  /**
   * Returns the last index applied to the state machine.
   *
   * @return The last index applied to the state machine.
   */
  public long getLastApplied() {
    return stateMachine.getLastApplied();
  }

  /**
   * Returns the last index completed for all sessions.
   *
   * @return The last index completed for all sessions.
   */
  public long getLastCompleted() {
    return stateMachine.getLastCompleted();
  }

  /**
   * Returns the current state.
   *
   * @return The current state.
   */
  public CopycatServer.State getState() {
    return state.type();
  }

  /**
   * Returns the state log.
   *
   * @return The state log.
   */
  public Log getLog() {
    return log;
  }

  /**
   * Checks that the current thread is the state context thread.
   */
  void checkThread() {
    threadContext.checkThread();
  }

  /**
   * Handles a connection from a client.
   */
  void connectClient(Connection connection) {
    threadContext.checkThread();

    // Note we do not use method references here because the "state" variable changes over time.
    // We have to use lambdas to ensure the request handler points to the current state.
    connection.handler(RegisterRequest.class, request -> state.register(request));
    connection.handler(ConnectRequest.class, request -> state.connect(request, connection));
    connection.handler(KeepAliveRequest.class, request -> state.keepAlive(request));
    connection.handler(UnregisterRequest.class, request -> state.unregister(request));
    connection.handler(CommandRequest.class, request -> state.command(request));
    connection.handler(QueryRequest.class, request -> state.query(request));

    connection.closeListener(stateMachine.executor().context().sessions()::unregisterConnection);
  }

  /**
   * Handles a connection from another server.
   */
  void connectServer(Connection connection) {
    threadContext.checkThread();

    // Handlers for all request types are registered since requests can be proxied between servers.
    // Note we do not use method references here because the "state" variable changes over time.
    // We have to use lambdas to ensure the request handler points to the current state.
    connection.handler(RegisterRequest.class, request -> state.register(request));
    connection.handler(ConnectRequest.class, request -> state.connect(request, connection));
    connection.handler(AcceptRequest.class, request -> state.accept(request));
    connection.handler(KeepAliveRequest.class, request -> state.keepAlive(request));
    connection.handler(UnregisterRequest.class, request -> state.unregister(request));
    connection.handler(PublishRequest.class, request -> state.publish(request));
    connection.handler(JoinRequest.class, request -> state.join(request));
    connection.handler(LeaveRequest.class, request -> state.leave(request));
    connection.handler(AppendRequest.class, request -> state.append(request));
    connection.handler(PollRequest.class, request -> state.poll(request));
    connection.handler(VoteRequest.class, request -> state.vote(request));
    connection.handler(CommandRequest.class, request -> state.command(request));
    connection.handler(QueryRequest.class, request -> state.query(request));
  }

  /**
   * Transition handler.
   */
  public CompletableFuture<CopycatServer.State> transition(CopycatServer.State state) {
    checkThread();

    if (this.state != null && state == this.state.type()) {
      return CompletableFuture.completedFuture(this.state.type());
    }

    LOGGER.info("{} - Transitioning to {}", cluster.getMember().serverAddress(), state);

    if (this.state != null) {
      try {
        this.state.close().get();
      } catch (InterruptedException | ExecutionException e) {
        throw new IllegalStateException("failed to close Raft state", e);
      }
    }

    // Force state transitions to occur synchronously in order to prevent race conditions.
    try {
      this.state = createState(state);
      this.state.open().get();
    } catch (InterruptedException | ExecutionException e) {
      throw new IllegalStateException("failed to initialize Raft state", e);
    }

    stateChangeListeners.forEach(l -> l.accept(this.state.type()));
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Joins the cluster.
   */
  public CompletableFuture<Void> join() {
    CompletableFuture<Void> future = new CompletableFuture<>();

    List<MemberState> votingMembers = cluster.getRemoteMemberStates(RaftMemberType.ACTIVE);
    if (votingMembers.isEmpty()) {
      LOGGER.debug("{} - Single member cluster. Transitioning directly to leader.", cluster.getMember().serverAddress());
      term++;
      transition(CopycatServer.State.LEADER);
      future.complete(null);
    } else {
      joinTimer = threadContext.schedule(electionTimeout, () -> {
        cluster.getMember().update(RaftMemberType.ACTIVE);
        transition(CopycatServer.State.FOLLOWER);
        future.complete(null);
      });

      join(cluster.getRemoteMembers(RaftMemberType.ACTIVE).iterator(), future);
    }

    return future;
  }

  /**
   * Recursively attempts to join the cluster.
   */
  private void join(Iterator<Member> iterator, CompletableFuture<Void> future) {
    if (iterator.hasNext()) {
      Member member = iterator.next();
      LOGGER.debug("{} - Attempting to join via {}", cluster.getMember().serverAddress(), member.serverAddress());

      connections.getConnection(member.serverAddress()).thenCompose(connection -> {
        JoinRequest request = JoinRequest.builder()
          .withMember(cluster.getMember())
          .build();
        return connection.<JoinRequest, JoinResponse>send(request);
      }).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status() == Response.Status.OK) {
            LOGGER.info("{} - Successfully joined via {}", cluster.getMember().serverAddress(), member.serverAddress());

            cluster.configure(response.version(), response.members());

            if (cluster.getMember().type() == RaftMemberType.ACTIVE) {
              cancelJoinTimer();
              transition(CopycatServer.State.FOLLOWER);
              future.complete(null);
            } else if (cluster.getMember().type() == RaftMemberType.PASSIVE) {
              cancelJoinTimer();
              transition(CopycatServer.State.PASSIVE);
              future.complete(null);
            } else {
              future.completeExceptionally(new IllegalStateException("not a member of the cluster"));
            }
          } else if (response.error() == null) {
            // If the response error is null, that indicates that no error occurred but the leader was
            // in a state that was incapable of handling the join request. Attempt to join the leader
            // again after an election timeout.
            LOGGER.debug("{} - Failed to join {}", cluster.getMember().serverAddress(), member.serverAddress());
            cancelJoinTimer();
            joinTimer = threadContext.schedule(electionTimeout, this::join);
          } else {
            // If the response error was non-null, attempt to join via the next server in the members list.
            LOGGER.debug("{} - Failed to join {}", cluster.getMember().serverAddress(), member.serverAddress());
            join(iterator, future);
          }
        } else {
          LOGGER.debug("{} - Failed to join {}", cluster.getMember().serverAddress(), member.serverAddress());
          join(iterator, future);
        }
      });
    } else {
      LOGGER.info("{} - Failed to join existing cluster", cluster.getMember().serverAddress());
      cluster.getMember().update(RaftMemberType.ACTIVE);
      cancelJoinTimer();
      transition(CopycatServer.State.FOLLOWER);
      future.complete(null);
    }
  }

  /**
   * Cancels the join timeout.
   */
  private void cancelJoinTimer() {
    if (joinTimer != null) {
      LOGGER.debug("{} - Cancelling join timeout", cluster.getMember().serverAddress());
      joinTimer.cancel();
      joinTimer = null;
    }
  }

  /**
   * Leaves the cluster.
   */
  public CompletableFuture<Void> leave() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    threadContext.execute(() -> {
      if (cluster.getRemoteMemberStates(RaftMemberType.ACTIVE).isEmpty()) {
        LOGGER.debug("{} - Single member cluster. Transitioning directly to inactive.", cluster.getMember().serverAddress());
        transition(RaftServer.State.INACTIVE);
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
    leaveTimer = threadContext.schedule(electionTimeout, () -> {
      leave(future);
    });

    // Attempt to leave the cluster by submitting a LeaveRequest directly to the server state.
    // Non-leader states should forward the request to the leader if there is one. Leader states
    // will log, replicate, and commit the reconfiguration.
    state.leave(LeaveRequest.builder()
      .withMember(cluster.getMember())
      .build()).whenComplete((response, error) -> {
      if (error == null && response.status() == Response.Status.OK) {
        cancelLeaveTimer();
        cluster.configure(response.version(), response.members());
        transition(RaftServer.State.INACTIVE);
        future.complete(null);
      }
    });
  }

  /**
   * Cancels the leave timeout.
   */
  private void cancelLeaveTimer() {
    if (leaveTimer != null) {
      LOGGER.debug("{} - Cancelling leave timeout", cluster.getMember().serverAddress());
      leaveTimer.cancel();
      leaveTimer = null;
    }
  }

  /**
   * Creates an internal state for the given state type.
   */
  private AbstractState createState(CopycatServer.State state) {
    switch (state) {
      case INACTIVE:
        return new InactiveState(this);
      case PASSIVE:
        return new PassiveState(this);
      case FOLLOWER:
        return new FollowerState(this);
      case CANDIDATE:
        return new CandidateState(this);
      case LEADER:
        return new LeaderState(this);
      default:
        throw new AssertionError();
    }
  }

  @Override
  public String toString() {
    return getClass().getCanonicalName();
  }

}
