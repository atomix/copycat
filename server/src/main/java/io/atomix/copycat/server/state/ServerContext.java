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
import io.atomix.catalyst.util.concurrent.SingleThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.request.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.cluster.Cluster;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.request.*;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.compaction.Compaction;
import io.atomix.copycat.server.storage.snapshot.SnapshotStore;
import io.atomix.copycat.server.storage.system.Configuration;
import io.atomix.copycat.server.storage.system.MetaStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Raft server state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ServerContext implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerContext.class);
  private final Listeners<CopycatServer.State> stateChangeListeners = new Listeners<>();
  private final Listeners<Member> electionListeners = new Listeners<>();
  private final String name;
  private final ThreadContext threadContext;
  private final StateMachine userStateMachine;
  private final ClusterState cluster;
  private final Storage storage;
  private final MetaStore meta;
  private Log log;
  private final SnapshotStore snapshot;
  private final ServerStateMachine stateMachine;
  private final ConnectionManager connections;
  private AbstractState state = new InactiveState(this);
  private Duration electionTimeout = Duration.ofMillis(500);
  private Duration sessionTimeout = Duration.ofMillis(5000);
  private Duration heartbeatInterval = Duration.ofMillis(150);
  private int leader;
  private long term;
  private int lastVotedFor;
  private long commitIndex;
  private long globalIndex;

  @SuppressWarnings("unchecked")
  public ServerContext(String name, Member.Type type, Address serverAddress, Address clientAddress, Collection<Address> members, Storage storage, StateMachine stateMachine, ConnectionManager connections, ThreadContext threadContext) {
    this.name = Assert.notNull(name, "name");
    ServerMember member = new ServerMember(Member.Type.INACTIVE, serverAddress, clientAddress);
    this.cluster = new ClusterState(this, member, type);
    this.storage = Assert.notNull(storage, "storage");
    this.threadContext = Assert.notNull(threadContext, "threadContext");
    this.connections = Assert.notNull(connections, "connections");
    this.userStateMachine = Assert.notNull(stateMachine, "stateMachine");

    // Open the meta store.
    this.meta = storage.openMetaStore(name);

    // Open the snapshot store.
    this.snapshot = storage.openSnapshotStore(name);

    // Open the log.
    resetLog();

    // Create a state machine executor and configure the state machine.
    ThreadContext stateContext = new SingleThreadContext("copycat-server-" + member.serverAddress() + "-state-%d", threadContext.serializer().clone());
    this.stateMachine = new ServerStateMachine(userStateMachine, this, stateContext);

    // Load the current term and last vote from disk.
    this.term = meta.loadTerm();
    this.lastVotedFor = meta.loadVote();

    // If a configuration is stored, use the stored configuration, otherwise configure the server with the user provided configuration.
    Configuration configuration = meta.loadConfiguration();
    if (configuration != null) {
      LOGGER.info("{} - Loaded {}", cluster.member().address(), configuration);
      cluster.configure(configuration.index(), configuration.members());
    } else if (members.contains(member.serverAddress())) {
      Set<Member> activeMembers = members.stream().filter(m -> !m.equals(member.serverAddress())).map(m -> new ServerMember(Member.Type.ACTIVE, m, null)).collect(Collectors.toSet());
      activeMembers.add(new ServerMember(Member.Type.ACTIVE, member.serverAddress(), member.clientAddress()));
      cluster.configure(0, activeMembers);
    } else {
      Set<Member> activeMembers = members.stream().map(m -> new ServerMember(Member.Type.ACTIVE, m, null)).collect(Collectors.toSet());
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
  public Listener<Member> onLeaderElection(Consumer<Member> listener) {
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
   * Returns the server storage.
   *
   * @return The server storage.
   */
  public Storage getStorage() {
    return storage;
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
  public ServerContext setElectionTimeout(Duration electionTimeout) {
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
  public ServerContext setHeartbeatInterval(Duration heartbeatInterval) {
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
  public ServerContext setSessionTimeout(Duration sessionTimeout) {
    this.sessionTimeout = sessionTimeout;
    return this;
  }

  /**
   * Sets the state leader.
   *
   * @param leader The state leader.
   * @return The Raft context.
   */
  ServerContext setLeader(int leader) {
    if (this.leader != leader) {
      // 0 indicates no leader.
      if (leader == 0) {
        this.leader = 0;
      } else {
        // If a valid leader ID was specified, it must be a member that's currently a member of the
        // ACTIVE members configuration. Note that we don't throw exceptions for unknown members. It's
        // possible that a failure following a configuration change could result in an unknown leader
        // sending AppendRequest to this server. Simply configure the leader if it's known.
        ServerMember member = cluster.member(leader);
        if (member != null) {
          this.leader = leader;
          LOGGER.info("{} - Found leader {}", cluster.member().address(), member.address());
          electionListeners.forEach(l -> l.accept(member));
          cluster.identify();
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
  public Cluster getCluster() {
    return cluster;
  }

  /**
   * Returns the cluster state.
   *
   * @return The cluster state.
   */
  ClusterState getClusterState() {
    return cluster;
  }

  /**
   * Returns the state leader.
   *
   * @return The state leader.
   */
  ServerMember getLeader() {
    if (leader == 0) {
      return null;
    }
    return cluster.member(leader);
  }

  /**
   * Sets the state term.
   *
   * @param term The state term.
   * @return The Raft context.
   */
  ServerContext setTerm(long term) {
    if (term > this.term) {
      this.term = term;
      this.leader = 0;
      this.lastVotedFor = 0;
      meta.storeTerm(this.term);
      meta.storeVote(this.lastVotedFor);
      LOGGER.debug("{} - Set term {}", cluster.member().address(), term);
    }
    return this;
  }

  /**
   * Returns the state term.
   *
   * @return The state term.
   */
  long getTerm() {
    return term;
  }

  /**
   * Sets the state last voted for candidate.
   *
   * @param candidate The candidate that was voted for.
   * @return The Raft context.
   */
  ServerContext setLastVotedFor(int candidate) {
    // If we've already voted for another candidate in this term then the last voted for candidate cannot be overridden.
    Assert.stateNot(lastVotedFor != 0 && candidate != 0l, "Already voted for another candidate");
    ServerMember member = cluster.member(candidate);
    Assert.state(member != null, "unknown candidate: %d", candidate);
    this.lastVotedFor = candidate;
    meta.storeVote(this.lastVotedFor);

    if (candidate != 0) {
      LOGGER.debug("{} - Voted for {}", cluster.member().address(), member.address());
    } else {
      LOGGER.debug("{} - Reset last voted for", cluster.member().address());
    }
    return this;
  }

  /**
   * Returns the state last voted for candidate.
   *
   * @return The state last voted for candidate.
   */
  int getLastVotedFor() {
    return lastVotedFor;
  }

  /**
   * Sets the commit index.
   *
   * @param commitIndex The commit index.
   * @return The Raft context.
   */
  ServerContext setCommitIndex(long commitIndex) {
    Assert.argNot(commitIndex < 0, "commit index must be positive");
    Assert.argNot(commitIndex < this.commitIndex, "cannot decrease commit index");
    this.commitIndex = commitIndex;
    log.commit(Math.min(commitIndex, log.lastIndex()));
    return this;
  }

  /**
   * Returns the commit index.
   *
   * @return The commit index.
   */
  long getCommitIndex() {
    return commitIndex;
  }

  /**
   * Sets the global index.
   *
   * @param globalIndex The global index.
   * @return The Raft context.
   */
  ServerContext setGlobalIndex(long globalIndex) {
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
  long getGlobalIndex() {
    return globalIndex;
  }

  /**
   * Returns the server state machine.
   *
   * @return The server state machine.
   */
  public ServerStateMachine getStateMachine() {
    return stateMachine;
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
   * Returns the current server state.
   *
   * @return The current server state.
   */
  AbstractState getAbstractState() {
    return state;
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
   * Returns the server log.
   *
   * @return The server log.
   */
  public Log getLog() {
    return log;
  }

  /**
   * Resets the state log.
   *
   * @return The server context.
   */
  ServerContext resetLog() {
    if (log != null) {
      log.close();
      log.delete();
    }

    log = storage.openLog(name);

    // Configure the log compaction mode. If the state machine supports snapshotting, the default
    // compaction mode is SNAPSHOT, otherwise the default is SEQUENTIAL.
    if (userStateMachine instanceof Snapshottable) {
      log.compactor().withDefaultCompactionMode(Compaction.Mode.SNAPSHOT);
    } else {
      log.compactor().withDefaultCompactionMode(Compaction.Mode.SEQUENTIAL);
    }

    return this;
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
  public void connectClient(Connection connection) {
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
  public void connectServer(Connection connection) {
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
    connection.handler(ReconfigureRequest.class, request -> state.reconfigure(request));
    connection.handler(LeaveRequest.class, request -> state.leave(request));
    connection.handler(AppendRequest.class, request -> state.append(request));
    connection.handler(PollRequest.class, request -> state.poll(request));
    connection.handler(VoteRequest.class, request -> state.vote(request));
    connection.handler(CommandRequest.class, request -> state.command(request));
    connection.handler(QueryRequest.class, request -> state.query(request));

    connection.closeListener(stateMachine.executor().context().sessions()::unregisterConnection);
  }

  /**
   * Transition handler.
   */
  public void transition(CopycatServer.State state) {
    checkThread();

    if (this.state != null && state == this.state.type()) {
      return;
    }

    LOGGER.info("{} - Transitioning to {}", cluster.member().address(), state);

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
  }

  /**
   * Creates an internal state for the given state type.
   */
  private AbstractState createState(CopycatServer.State state) {
    switch (state) {
      case INACTIVE:
        return new InactiveState(this);
      case RESERVE:
        return new ReserveState(this);
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
  public void close() {
    try {
      log.close();
    } catch (Exception e) {
    }
    try {
      meta.close();
    } catch (Exception e) {
    }
    try {
      snapshot.close();
    } catch (Exception e) {
    }
    stateMachine.close();
    threadContext.close();
  }

  /**
   * Deletes the server context.
   */
  public void delete() {
    // Delete the metadata store.
    MetaStore meta = storage.openMetaStore(name);
    meta.close();
    meta.delete();

    // Delete the log.
    Log log = storage.openLog(name);
    log.close();
    log.delete();

    // Delete the snapshot store.
    SnapshotStore snapshot = storage.openSnapshotStore(name);
    snapshot.close();
    snapshot.delete();
  }

  @Override
  public String toString() {
    return getClass().getCanonicalName();
  }

}
