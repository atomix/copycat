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
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.request.*;
import io.atomix.copycat.server.response.JoinResponse;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.snapshot.SnapshotStore;
import io.atomix.copycat.server.storage.system.Configuration;
import io.atomix.copycat.server.storage.system.MetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Iterator;
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
  private static final int MAX_JOIN_ATTEMPTS = 3;
  private final Listeners<CopycatServer.State> stateChangeListeners = new Listeners<>();
  private final Listeners<Address> electionListeners = new Listeners<>();
  private ThreadContext threadContext;
  private final StateMachine userStateMachine;
  private final ClusterState cluster;
  private final MetaStore meta;
  private final Log log;
  private final SnapshotStore snapshot;
  private final ServerStateMachine stateMachine;
  private final ConnectionManager connections;
  private AbstractState state = new InactiveState(this);
  private Duration electionTimeout = Duration.ofMillis(500);
  private Duration sessionTimeout = Duration.ofMillis(5000);
  private Duration heartbeatInterval = Duration.ofMillis(150);
  private Scheduled joinTimer;
  private CompletableFuture<Void> joinFuture;
  private Scheduled leaveTimer;
  private int leader;
  private long term;
  private int lastVotedFor;
  private long commitIndex;
  private long globalIndex;

  @SuppressWarnings("unchecked")
  ServerState(Member member, Collection<Address> members, MetaStore meta, Log log, SnapshotStore snapshot, StateMachine stateMachine, ConnectionManager connections, ThreadContext threadContext) {
    this.cluster = new ClusterState(this, member);
    this.meta = Assert.notNull(meta, "meta");
    this.log = Assert.notNull(log, "log");
    this.snapshot = Assert.notNull(snapshot, "data");
    this.threadContext = Assert.notNull(threadContext, "threadContext");
    this.connections = Assert.notNull(connections, "connections");
    this.userStateMachine = Assert.notNull(stateMachine, "stateMachine");

    // Create a state machine executor and configure the state machine.
    ThreadContext stateContext = new SingleThreadContext("copycat-server-" + member.serverAddress() + "-state-%d", threadContext.serializer().clone());
    this.stateMachine = new ServerStateMachine(userStateMachine, this, stateContext);

    // Load the current term and last vote from disk.
    this.term = meta.loadTerm();
    this.lastVotedFor = meta.loadVote();

    // If a configuration is stored, use the stored configuration, otherwise configure the server with the user provided configuration.
    Configuration configuration = meta.loadConfiguration();
    if (configuration != null) {
      cluster.configure(configuration.index(), configuration.members());
    } else if (members.contains(member.serverAddress())) {
      Set<Member> activeMembers = members.stream().filter(m -> !m.equals(member.serverAddress())).map(m -> new Member(CopycatServer.Type.ACTIVE, m, null)).collect(Collectors.toSet());
      activeMembers.add(new Member(CopycatServer.Type.ACTIVE, member.serverAddress(), member.clientAddress()));
      cluster.configure(0, activeMembers);
    } else {
      Set<Member> activeMembers = members.stream().map(m -> new Member(CopycatServer.Type.ACTIVE, m, null)).collect(Collectors.toSet());
      cluster.configure(0, activeMembers);
    }
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
    if (this.leader != leader) {
      // 0 indicates no leader.
      if (leader == 0) {
        this.leader = 0;
      } else {
        // If a valid leader ID was specified, it must be a member that's currently a member of the
        // ACTIVE members configuration. Note that we don't throw exceptions for unknown members. It's
        // possible that a failure following a configuration change could result in an unknown leader
        // sending AppendRequest to this server. Simply configure the leader if it's known.
        Member member = cluster.getMember(leader);
        if (member != null) {
          this.leader = leader;
          LOGGER.info("{} - Found leader {}", cluster.getMember().serverAddress(), member.serverAddress());
          electionListeners.forEach(l -> l.accept(member.serverAddress()));
          joinLeader(member);
        }
      }

      this.lastVotedFor = 0;
      meta.storeVote(0);
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
  public Member getLeader() {
    if (leader == 0) {
      return null;
    }
    return cluster.getMember(leader);
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
      meta.storeTerm(this.term);
      meta.storeVote(this.lastVotedFor);
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
    Assert.stateNot(leader != 0 && candidate != 0, "Cannot cast vote - leader already exists");
    Member member = cluster.getMember(candidate);
    Assert.state(member != null, "unknown candidate: %d", candidate);
    this.lastVotedFor = candidate;
    meta.storeVote(this.lastVotedFor);

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
    log.commit(commitIndex);
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
   * Sets the global index.
   *
   * @param globalIndex The global index.
   * @return The Raft context.
   */
  ServerState setGlobalIndex(long globalIndex) {
    Assert.argNot(globalIndex < 0, "global index must be positive");
    this.globalIndex = Math.max(this.globalIndex, globalIndex);
    log.compactor().majorIndex(globalIndex);
    return this;
  }

  /**
   * Returns the global index.
   *
   * @return The global index.
   */
  public long getGlobalIndex() {
    return globalIndex;
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
   * Returns the current local member type.
   *
   * @return The current local member type.
   */
  public CopycatServer.Type getType() {
    return cluster.getMember().type();
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
   * Returns the server metadata store.
   *
   * @return The server metadata store.
   */
  public MetaStore getMetaStore() {
    return meta;
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
   * Returns the server snapshot store.
   *
   * @return The server snapshot store.
   */
  public SnapshotStore getSnapshotStore() {
    return snapshot;
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
    connection.handler(ConfigureRequest.class, request -> state.configure(request));
    connection.handler(InstallRequest.class, request -> state.install(request));
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
    if (joinFuture != null)
      return joinFuture;

    joinFuture = new CompletableFuture<>();

    // If the server type is defined, that indicates it's a member of the current configuration and
    // doesn't need to be added to the configuration. Immediately transition to the appropriate state.
    // Note that we don't complete the join future when transitioning to a valid state since we need
    // to ensure that the server's configuration has been updated in the cluster before completing the join.
    if (cluster.getMember().type() != CopycatServer.Type.INACTIVE) {
      if (cluster.getMember().type() == CopycatServer.Type.ACTIVE) {
        transition(CopycatServer.State.FOLLOWER);
      } else if (cluster.getMember().type() == CopycatServer.Type.PASSIVE) {
        transition(CopycatServer.State.PASSIVE);
      } else {
        joinFuture.completeExceptionally(new IllegalStateException("unknown member type: " + cluster.getMember().type()));
      }
    } else {
      join(cluster.getRemoteMemberStates(CopycatServer.Type.ACTIVE).iterator(), 1);
    }

    return joinFuture.whenComplete((result, error) -> joinFuture = null);
  }

  /**
   * Recursively attempts to join the cluster.
   */
  private void join(Iterator<MemberState> iterator, int attempts) {
    if (iterator.hasNext()) {
      MemberState member = iterator.next();
      LOGGER.debug("{} - Attempting to join via {}", cluster.getMember().serverAddress(), member.getMember().serverAddress());

      connections.getConnection(member.getMember().serverAddress()).thenCompose(connection -> {
        JoinRequest request = JoinRequest.builder()
          .withMember(cluster.getMember())
          .build();
        return connection.<JoinRequest, JoinResponse>send(request);
      }).whenComplete((response, error) -> {
        if (error == null) {
          if (response.status() == Response.Status.OK) {
            LOGGER.info("{} - Successfully joined via {}", cluster.getMember().serverAddress(), member.getMember().serverAddress());

            // Configure the cluster with the join response.
            cluster.configure(response.index(), response.members());

            // Cancel the join timer.
            cancelJoinTimer();

            // If the local member type is null, that indicates it's not a part of the configuration.
            CopycatServer.Type type = cluster.getMember().type();
            if (type == null) {
              joinFuture.completeExceptionally(new IllegalStateException("not a member of the cluster"));
            } else if (type == CopycatServer.Type.ACTIVE) {
              transition(CopycatServer.State.FOLLOWER);
              joinFuture.complete(null);
            } else if (type == CopycatServer.Type.PASSIVE) {
              transition(CopycatServer.State.PASSIVE);
              joinFuture.complete(null);
            } else {
              joinFuture.completeExceptionally(new IllegalStateException("unknown member type: " + type));
            }
          } else if (response.error() == null) {
            // If the response error is null, that indicates that no error occurred but the leader was
            // in a state that was incapable of handling the join request. Attempt to join the leader
            // again after an election timeout.
            LOGGER.debug("{} - Failed to join {}", cluster.getMember().serverAddress(), member.getMember().serverAddress());
            cancelJoinTimer();
            joinTimer = threadContext.schedule(electionTimeout, () -> {
              join(cluster.getRemoteMemberStates(CopycatServer.Type.ACTIVE).iterator(), attempts);
            });
          } else {
            // If the response error was non-null, attempt to join via the next server in the members list.
            LOGGER.debug("{} - Failed to join {}", cluster.getMember().serverAddress(), member.getMember().serverAddress());
            join(iterator, attempts);
          }
        } else {
          LOGGER.debug("{} - Failed to join {}", cluster.getMember().serverAddress(), member.getMember().serverAddress());
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
      joinTimer = threadContext.schedule(electionTimeout.multipliedBy(2), () -> {
        join(cluster.getRemoteMemberStates(CopycatServer.Type.ACTIVE).iterator(), attempts + 1);
      });
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
   * Sends a join request to the given leader once found.
   */
  private void joinLeader(Member leader) {
    if (joinFuture != null) {
      if (cluster.getMember().equals(leader)) {
        if (state.type() == CopycatServer.State.LEADER && !((LeaderState) state).configuring()) {
          joinFuture.complete(null);
        } else {
          cancelJoinTimer();
          joinTimer = threadContext.schedule(electionTimeout.multipliedBy(2), () -> {
            joinLeader(leader);
          });
        }
      } else {
        LOGGER.debug("{} - Sending server identification to {}", cluster.getMember().serverAddress(), leader.serverAddress());
        connections.getConnection(leader.serverAddress()).thenCompose(connection -> {
          JoinRequest request = JoinRequest.builder()
            .withMember(cluster.getMember())
            .build();
          return connection.<JoinRequest, JoinResponse>send(request);
        }).whenComplete((response, error) -> {
          if (error == null) {
            if (response.status() == Response.Status.OK) {
              cancelJoinTimer();
              if (joinFuture != null)
                joinFuture.complete(null);
            } else if (response.error() == null) {
              cancelJoinTimer();
              joinTimer = threadContext.schedule(electionTimeout.multipliedBy(2), () -> {
                joinLeader(leader);
              });
            }
          }
        });
      }
    }
  }

  /**
   * Leaves the cluster.
   */
  public CompletableFuture<Void> leave() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    threadContext.execute(() -> {
      if (cluster.getRemoteMemberStates(CopycatServer.Type.ACTIVE).isEmpty()) {
        LOGGER.debug("{} - Single member cluster. Transitioning directly to inactive.", cluster.getMember().serverAddress());
        transition(CopycatServer.State.INACTIVE);
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
        cluster.configure(response.index(), response.members());
        transition(CopycatServer.State.INACTIVE);
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
