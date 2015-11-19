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

import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.concurrent.ComposableFuture;
import io.atomix.catalyst.util.concurrent.Scheduled;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Query;
import io.atomix.copycat.client.error.RaftError;
import io.atomix.copycat.client.error.RaftException;
import io.atomix.copycat.client.request.*;
import io.atomix.copycat.client.response.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.request.*;
import io.atomix.copycat.server.response.*;
import io.atomix.copycat.server.storage.entry.*;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Leader state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class LeaderState extends ActiveState {
  private final LeaderAppender appender;
  private final MembershipRebalancer rebalancer;
  private Scheduled appendTimer;
  private long configuring;
  private boolean changed;
  private CompletableFuture<Void> rebalanceFuture;

  public LeaderState(ServerState context) {
    super(context);
    this.appender = new LeaderAppender(context);
    this.rebalancer = new MembershipRebalancer(context);
  }

  @Override
  public CopycatServer.State type() {
    return CopycatServer.State.LEADER;
  }

  @Override
  public synchronized CompletableFuture<AbstractState> open() {
    // Append initial entries to the log, including an initial no-op entry and the server's configuration.
    appendInitialEntries();

    // Schedule the initial entries commit to occur after the state is opened. Attempting any communication
    // within the open() method will result in a deadlock since RaftProtocol calls this method synchronously.
    // What is critical about this logic is that the append timer not be started until a no-op entry has been committed.
    context.getThreadContext().execute(this::commitInitialEntries).whenComplete((result, error) -> {
      if (isOpen() && error == null) {
        startAppendTimer();
      }
    });

    return super.open()
      .thenRun(this::takeLeadership)
      .thenApply(v -> this);
  }

  /**
   * Sets the current node as the cluster leader.
   */
  private void takeLeadership() {
    context.setLeader(context.getMember().id());
    context.getMemberStates().forEach(m -> m.resetState(context.getLog()));
  }

  /**
   * Appends initial entries to the log to take leadership.
   */
  private void appendInitialEntries() {
    final long term = context.getTerm();

    // Append a no-op entry to reset session timeouts and commit entries from prior terms.
    try (NoOpEntry entry = context.getLog().create(NoOpEntry.class)) {
      entry.setTerm(term)
        .setTimestamp(appender.time());
      assert context.getLog().append(entry) == appender.index();
    }

    // Append a configuration entry to propagate the leader's cluster configuration.
    try (ConfigurationEntry entry = context.getLog().create(ConfigurationEntry.class)) {
      entry.setTerm(term)
        .setActiveMembers(context.buildActiveMembers())
        .setPassiveMembers(context.buildPassiveMembers())
        .setReserveMembers(context.buildReserveMembers());
    }
  }

  /**
   * Commits a no-op entry to the log, ensuring any entries from a previous term are committed.
   */
  private CompletableFuture<Void> commitInitialEntries() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    appender.appendEntries(appender.index()).whenComplete((resultIndex, error) -> {
      context.checkThread();
      if (isOpen()) {
        if (error == null) {
          applyEntries(resultIndex);
          future.complete(null);
        } else {
          context.setLeader(0);
          transition(CopycatServer.State.FOLLOWER);
        }
      }
    });
    return future;
  }

  /**
   * Applies all unapplied entries to the log.
   */
  private void applyEntries(long index) {
    if (!context.getLog().isEmpty()) {
      int count = 0;
      for (long lastApplied = Math.max(context.getLastApplied(), context.getLog().firstIndex()); lastApplied <= index; lastApplied++) {
        Entry entry = context.getLog().get(lastApplied);
        if (entry != null) {
          context.getStateMachine().apply(entry).whenComplete((result, error) -> {
            if (isOpen() && error != null) {
              LOGGER.info("{} - An application error occurred: {}", context.getMember().serverAddress(), error.getMessage());
            }
            entry.release();
          });
        }
        count++;
      }

      LOGGER.debug("{} - Applied {} entries to log", context.getMember().serverAddress(), count);
      context.getLog().compactor().minorIndex(context.getLastCompleted());
    }
  }

  /**
   * Starts sending AppendEntries requests to all cluster members.
   */
  private void startAppendTimer() {
    // Set a timer that will be used to periodically synchronize with other nodes
    // in the cluster. This timer acts as a heartbeat to ensure this node remains
    // the leader.
    LOGGER.debug("{} - Starting append timer", context.getMember().serverAddress());
    appendTimer = context.getThreadContext().schedule(Duration.ZERO, context.getHeartbeatInterval(), this::appendMembers);
  }

  /**
   * Sends AppendEntries requests to members of the cluster that haven't heard from the leader in a while.
   */
  private void appendMembers() {
    context.checkThread();
    if (isOpen()) {
      appender.appendEntries().whenComplete((result, error) -> {
        context.getLog().compactor().minorIndex(context.getLastCompleted());
      });
    }
  }

  /**
   * Checks for expired sessions.
   */
  private void checkSessions() {
    long term = context.getTerm();
    for (ServerSession session : context.getStateMachine().executor().context().sessions().sessions.values()) {
      if (!session.isUnregistering() && session.isSuspect()) {
        LOGGER.debug("{} - Detected expired session: {}", context.getMember().serverAddress(), session.id());

        final long index;
        try (UnregisterEntry entry = context.getLog().create(UnregisterEntry.class)) {
          entry.setTerm(term)
            .setSession(session.id())
            .setExpired(true)
            .setTimestamp(System.currentTimeMillis());
          index = context.getLog().append(entry);
          LOGGER.debug("{} - Appended {}", context.getMember().serverAddress(), entry);
        }

        appender.appendEntries(index).whenComplete((result, error) -> {
          if (isOpen()) {
            UnregisterEntry entry = context.getLog().get(index);
            LOGGER.debug("{} - Applying {}", context.getMember().serverAddress(), entry);
            context.getStateMachine().apply(entry, true).whenComplete((unregisterResult, unregisterError) -> entry.release());
          }
        });
        session.unregister();
      }
    }
  }

  @Override
  public CompletableFuture<JoinResponse> join(final JoinRequest request) {
    context.checkThread();
    logRequest(request);

    // If another configuration change is already under way, reject the configuration.
    if (configuring > 0) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
        .withStatus(Response.Status.ERROR)
        .build()));
    }

    // If the leader index is 0 or is greater than the commitIndex, reject the join requests.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    if (appender.index() == 0 || context.getCommitIndex() < appender.index()) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
        .withStatus(Response.Status.ERROR)
        .build()));
    }

    // If the member is already a known member of the cluster, complete the join successfully.
    MemberState existingMember = context.getMemberState(request.member().id());
    if (existingMember != null) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
        .withStatus(Response.Status.OK)
        .withVersion(context.getVersion())
        .withActiveMembers(context.buildActiveMembers())
        .withPassiveMembers(context.buildPassiveMembers())
        .withReserveMembers(context.buildReserveMembers())
        .build()));
    }

    final long term = context.getTerm();
    final long index;

    Collection<Member> activeMembers = context.buildActiveMembers();
    Collection<Member> passiveMembers = context.buildPassiveMembers();
    Collection<Member> reserveMembers = context.buildReserveMembers();

    // Add the server to the reserve members list.
    reserveMembers.add(request.member());

    try (ConfigurationEntry entry = context.getLog().create(ConfigurationEntry.class)) {
      entry.setTerm(term)
        .setActiveMembers(activeMembers)
        .setPassiveMembers(passiveMembers)
        .setReserveMembers(reserveMembers);
      index = context.getLog().append(entry);
      LOGGER.debug("{} - Appended {}", context.getMember().serverAddress(), entry);

      // Store the index of the configuration entry in order to prevent other configurations from
      // being logged and committed concurrently. This is an important safety property of Raft.
      configuring = index;
      changed = true;
      context.configure(entry.getIndex(), entry.getActiveMembers(), entry.getPassiveMembers(), entry.getReserveMembers());
    }

    CompletableFuture<JoinResponse> future = new CompletableFuture<>();
    appender.appendEntries(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        // Reset the configuration index to allow new configuration changes to be committed.
        reconfigure().whenComplete((reconfigureResult, reconfigureError) -> {
          if (commitError == null) {
            future.complete(logResponse(JoinResponse.builder()
              .withStatus(Response.Status.OK)
              .withVersion(index)
              .withActiveMembers(context.buildActiveMembers())
              .withPassiveMembers(context.buildPassiveMembers())
              .withReserveMembers(context.buildReserveMembers())
              .build()));
          } else {
            future.complete(logResponse(JoinResponse.builder()
              .withStatus(Response.Status.ERROR)
              .withError(RaftError.Type.INTERNAL_ERROR)
              .build()));
          }
        });
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(final LeaveRequest request) {
    context.checkThread();
    logRequest(request);

    // If another configuration change is already under way, reject the configuration.
    if (configuring > 0) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
        .withStatus(Response.Status.ERROR)
        .build()));
    }

    // If the leader index is 0 or is greater than the commitIndex, reject the join requests.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    if (appender.index() == 0 || context.getCommitIndex() < appender.index()) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
        .withStatus(Response.Status.ERROR)
        .build()));
    }

    // If the leaving member is not a known member of the cluster, complete the leave successfully.
    if (!request.member().equals(context.getMember()) && context.getMember(request.member().id()) == null) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
        .withStatus(Response.Status.OK)
        .withActiveMembers(context.buildActiveMembers())
        .withPassiveMembers(context.buildPassiveMembers())
        .withReserveMembers(context.buildReserveMembers())
        .build()));
    }

    final long term = context.getTerm();
    final long index;

    // Remove the member from the active members list if necessary.
    Collection<Member> activeMembers = context.buildActiveMembers();
    activeMembers.remove(request.member());

    // Remove the member from the passive members list if necessary.
    Collection<Member> passiveMembers = context.buildPassiveMembers();
    passiveMembers.remove(request.member());

    // Remove the member from the reserve members list if necessary.
    Collection<Member> reserveMembers = context.buildReserveMembers();
    reserveMembers.remove(request.member());

    try (ConfigurationEntry entry = context.getLog().create(ConfigurationEntry.class)) {
      entry.setTerm(term)
        .setActiveMembers(activeMembers)
        .setPassiveMembers(passiveMembers)
        .setReserveMembers(reserveMembers);
      index = context.getLog().append(entry);
      LOGGER.debug("{} - Appended {}", context.getMember().serverAddress(), entry);

      // Store the index of the configuration entry in order to prevent other configurations from
      // being logged and committed concurrently. This is an important safety property of Raft.
      configuring = index;
      changed = true;
      context.configure(entry.getIndex(), entry.getActiveMembers(), entry.getPassiveMembers(), entry.getReserveMembers());
    }

    CompletableFuture<LeaveResponse> future = new CompletableFuture<>();
    appender.appendEntries(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        // Reset the configuration index to allow new configuration changes to be committed.
        reconfigure().whenComplete((reconfigureResult, reconfigureError) -> {
          if (commitError == null) {
            future.complete(logResponse(LeaveResponse.builder()
              .withStatus(Response.Status.OK)
              .withVersion(index)
              .withActiveMembers(context.buildActiveMembers())
              .withPassiveMembers(context.buildPassiveMembers())
              .withReserveMembers(context.buildReserveMembers())
              .build()));
          } else {
            future.complete(logResponse(LeaveResponse.builder()
              .withStatus(Response.Status.ERROR)
              .withError(RaftError.Type.INTERNAL_ERROR)
              .build()));
          }
        });
      }
    });
    return future;
  }

  @Override
  protected CompletableFuture<HeartbeatResponse> heartbeat(HeartbeatRequest request) {
    final long timestamp = System.currentTimeMillis();
    final long index;

    context.checkThread();
    logRequest(request);

    MemberState member = context.getMemberState(request.member());
    if (member == null) {
      return CompletableFuture.completedFuture(logResponse(HeartbeatResponse.builder()
        .withStatus(Response.Status.ERROR)
        .build()));
    }

    // If the member client address has changed, append a new configuration.
    if (member.getMember().clientAddress() == null || !member.getMember().clientAddress().equals(request.member().clientAddress())) {
      member.setClientAddress(request.member().clientAddress());

      // Append a new configuration entry. Note that we don't need to ensure this configuration is not
      // committed concurrently with other configurations since it's not modifying the structure of the cluster.
      // This configuration update simply modifies the existing configuration, and new configurations will be
      // rejected if the current configuration is still being committed.
      try (ConfigurationEntry entry = context.getLog().create(ConfigurationEntry.class)) {
        entry.setTerm(context.getTerm())
          .setActiveMembers(context.buildActiveMembers())
          .setPassiveMembers(context.buildPassiveMembers())
          .setReserveMembers(context.buildReserveMembers());
        context.getLog().append(entry);
        LOGGER.debug("{} - Appended {}", context.getMember().serverAddress(), entry);
      }
    }

    try (HeartbeatEntry entry = context.getLog().create(HeartbeatEntry.class)) {
      entry.setTerm(context.getTerm())
        .setMember(request.member().id())
        .setCommitIndex(request.commitIndex())
        .setTimestamp(timestamp);
      index = context.getLog().append(entry);
      LOGGER.debug("{} - Appended {}", context.getMember().serverAddress(), entry);
    }

    CompletableFuture<HeartbeatResponse> future = new CompletableFuture<>();
    appender.appendEntries(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          HeartbeatEntry entry = context.getLog().get(index);
          applyEntry(entry).whenComplete((changed, error) -> {
            if (isOpen()) {
              if (error == null) {
                future.complete(logResponse(HeartbeatResponse.builder()
                  .withStatus(Response.Status.OK)
                  .withTerm(context.getTerm())
                  .withLeader(context.getMember().id())
                  .build()));

                // Once a heartbeat has been committed and applied to the state machine, if the heartbeat
                // causes the change of availability to any members, rebalance the cluster as necessary.
                if ((Boolean) changed) {
                  rebalance();
                }
              } else if (error instanceof RaftException) {
                future.complete(logResponse(HeartbeatResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withTerm(context.getTerm())
                  .withLeader(context.getMember().id())
                  .withError(((RaftException) error).getType())
                  .build()));
              } else {
                future.complete(logResponse(HeartbeatResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withTerm(context.getTerm())
                  .withLeader(context.getMember().id())
                  .withError(RaftError.Type.INTERNAL_ERROR)
                  .build()));
              }
            }
            entry.release();
          });
        } else {
          future.complete(logResponse(HeartbeatResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withTerm(context.getTerm())
            .withLeader(context.getMember().id())
            .withError(RaftError.Type.INTERNAL_ERROR)
            .build()));
        }
      }
    });
    return future;
  }

  /**
   * Resets the configuration index and rebalances the configuration if necessary.
   */
  private CompletableFuture<Void> reconfigure() {
    configuring = 0;
    if (changed) {
      changed = false;
      return rebalance();
    }
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Rebalances the cluster configuration according to member statuses.
   */
  private CompletableFuture<Void> rebalance() {
    if (rebalanceFuture == null)
      rebalanceFuture = new CompletableFuture<>();

    // If a configuration is already under way, just set a flag indicating that another configuration needs
    // to be appended once the configuration is complete.
    if (configuring != 0) {
      changed = true;
      return rebalanceFuture;
    }

    LOGGER.debug("{} - Rebalancing cluster", context.getMember().serverAddress());

    // Build lists of active, passive, and reserve members.
    List<Member> activeMembers = context.buildActiveMembers();
    List<Member> passiveMembers = context.buildPassiveMembers();
    List<Member> reserveMembers = context.buildReserveMembers();

    // If the overall configuration changed, log and commit a new configuration entry.
    if (rebalancer.rebalance(activeMembers, passiveMembers, reserveMembers)) {
      final long index;
      try (ConfigurationEntry entry = context.getLog().create(ConfigurationEntry.class)) {
        entry.setTerm(context.getTerm())
          .setActiveMembers(activeMembers)
          .setPassiveMembers(passiveMembers)
          .setReserveMembers(reserveMembers);
        index = context.getLog().append(entry);
        LOGGER.debug("{} - Appended {}", context.getMember().serverAddress(), entry);

        // Store the index of the configuration entry in order to prevent other configurations from
        // being logged and committed concurrently. This is an important safety property of Raft.
        configuring = index;
        context.configure(entry.getIndex(), entry.getActiveMembers(), entry.getPassiveMembers(), entry.getReserveMembers());
      }

      // Commit the configuration and then reset the configuration index to allow new configurations to proceed.
      appender.appendEntries(index).whenComplete((commitIndex, commitError) -> {
        context.checkThread();
        if (isOpen()) {
          rebalanceFuture.complete(null);
          rebalanceFuture = null;
          reconfigure();
        }
      });
    } else {
      rebalanceFuture.complete(null);
    }
    return rebalanceFuture;
  }

  @Override
  public CompletableFuture<PollResponse> poll(final PollRequest request) {
    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
      .withStatus(Response.Status.OK)
      .withTerm(context.getTerm())
      .withAccepted(false)
      .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> vote(final VoteRequest request) {
    if (request.term() > context.getTerm()) {
      LOGGER.debug("{} - Received greater term", context.getMember().serverAddress());
      context.setLeader(0);
      transition(CopycatServer.State.FOLLOWER);
      return super.vote(request);
    } else {
      return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withVoted(false)
        .build()));
    }
  }

  @Override
  public CompletableFuture<AppendResponse> append(final AppendRequest request) {
    context.checkThread();
    if (request.term() > context.getTerm()) {
      return super.append(request);
    } else if (request.term() < context.getTerm()) {
      return CompletableFuture.completedFuture(logResponse(AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getLog().lastIndex())
        .build()));
    } else {
      context.setLeader(request.leader());
      transition(CopycatServer.State.FOLLOWER);
      return super.append(request);
    }
  }

  @Override
  protected CompletableFuture<CommandResponse> command(final CommandRequest request) {
    context.checkThread();
    logRequest(request);

    // Get the client's server session. If the session doesn't exist, return an unknown session error.
    ServerSession session = context.getStateMachine().executor().context().sessions().getSession(request.session());
    if (session == null) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.UNKNOWN_SESSION_ERROR)
        .build()));
    }

    ComposableFuture<CommandResponse> future = new ComposableFuture<>();

    Command command = request.command();

    // If the command is LINEARIZABLE and the session's current sequence number is less then one prior to the request
    // sequence number, queue this request for handling later. We want to handle command requests in the order in which
    // they were sent by the client. Note that it's possible for the session sequence number to be greater than the request
    // sequence number. In that case, it's likely that the command was submitted more than once to the
    // cluster, and the command will be deduplicated once applied to the state machine.
    if (request.sequence() > session.nextRequest()) {
      session.registerRequest(request.sequence(), () -> command(request).whenComplete(future));
      return future;
    }

    final long term = context.getTerm();
    final long timestamp = System.currentTimeMillis();
    final long index;

    // Create a CommandEntry and append it to the log.
    try (CommandEntry entry = context.getLog().create(CommandEntry.class)) {
      entry.setTerm(term)
        .setSession(request.session())
        .setTimestamp(timestamp)
        .setSequence(request.sequence())
        .setCommand(command);
      index = context.getLog().append(entry);
      LOGGER.debug("{} - Appended {}", context.getMember().serverAddress(), entry);
    }

    appender.appendEntries(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          CommandEntry entry = context.getLog().get(index);

          LOGGER.debug("{} - Applying {}", context.getMember().serverAddress(), entry);
          context.getStateMachine().apply(entry, true).whenComplete((result, error) -> {
            if (isOpen()) {
              if (error == null) {
                future.complete(logResponse(CommandResponse.builder()
                  .withStatus(Response.Status.OK)
                  .withVersion(entry.getIndex())
                  .withResult(result)
                  .build()));
              } else if (error instanceof RaftException) {
                future.complete(logResponse(CommandResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withVersion(entry.getIndex())
                  .withError(((RaftException) error).getType())
                  .build()));
              } else {
                future.complete(logResponse(CommandResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withVersion(entry.getIndex())
                  .withError(RaftError.Type.INTERNAL_ERROR)
                  .build()));
              }
              checkSessions();
            }
            entry.release();
          });
        } else {
          future.complete(logResponse(CommandResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.INTERNAL_ERROR)
            .build()));
        }
      }
    });

    // Set the last processed request for the session. This will cause sequential command callbacks to be executed.
    session.setRequest(request.sequence());

    return future;
  }

  @Override
  protected CompletableFuture<QueryResponse> query(final QueryRequest request) {

    Query query = request.query();

    final long timestamp = System.currentTimeMillis();
    final long index = context.getCommitIndex();

    context.checkThread();
    logRequest(request);

    QueryEntry entry = context.getLog().create(QueryEntry.class)
      .setIndex(index)
      .setTerm(context.getTerm())
      .setTimestamp(timestamp)
      .setSession(request.session())
      .setSequence(request.sequence())
      .setVersion(request.version())
      .setQuery(query);

    Query.ConsistencyLevel consistency = query.consistency();
    if (consistency == null)
      return submitQueryLinearizable(entry);

    switch (consistency) {
      case CAUSAL:
      case SEQUENTIAL:
        return submitQueryLocal(entry);
      case BOUNDED_LINEARIZABLE:
        return submitQueryBoundedLinearizable(entry);
      case LINEARIZABLE:
        return submitQueryLinearizable(entry);
      default:
        throw new IllegalStateException("unknown consistency level");
    }
  }

  /**
   * Submits a query with serializable consistency.
   */
  private CompletableFuture<QueryResponse> submitQueryLocal(QueryEntry entry) {
    return applyQuery(entry, new CompletableFuture<>());
  }

  /**
   * Submits a query with lease bounded linearizable consistency.
   */
  private CompletableFuture<QueryResponse> submitQueryBoundedLinearizable(QueryEntry entry) {
    long commitTime = appender.time();
    if (System.currentTimeMillis() - commitTime < context.getElectionTimeout().toMillis()) {
      return submitQueryLocal(entry);
    } else {
      return submitQueryLinearizable(entry);
    }
  }

  /**
   * Submits a query with strict linearizable consistency.
   */
  private CompletableFuture<QueryResponse> submitQueryLinearizable(QueryEntry entry) {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    appender.appendEntries().whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          entry.acquire();
          applyQuery(entry, future);
        } else {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.COMMAND_ERROR)
            .build()));
        }
      }
      entry.release();
    });
    return future;
  }

  /**
   * Applies a query to the state machine.
   */
  private CompletableFuture<QueryResponse> applyQuery(QueryEntry entry, CompletableFuture<QueryResponse> future) {
    // In the case of the leader, the state machine is always up to date, so no queries will be queued and all query
    // versions will be the last applied index.
    final long version = context.getStateMachine().getLastApplied();
    applyEntry(entry).whenComplete((result, error) -> {
      if (isOpen()) {
        if (error == null) {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.OK)
            .withVersion(version)
            .withResult(result)
            .build()));
        } else if (error instanceof RaftException) {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(((RaftException) error).getType())
            .build()));
        } else {
          future.complete(logResponse(QueryResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.INTERNAL_ERROR)
            .build()));
        }
        checkSessions();
      }
      entry.release();
    });
    return future;
  }

  @Override
  protected CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    final long timestamp = System.currentTimeMillis();
    final long index;
    final long timeout = context.getSessionTimeout().toMillis();

    context.checkThread();
    logRequest(request);

    try (RegisterEntry entry = context.getLog().create(RegisterEntry.class)) {
      entry.setTerm(context.getTerm())
        .setTimestamp(timestamp)
        .setClient(request.client())
        .setTimeout(timeout);
      index = context.getLog().append(entry);
      LOGGER.debug("{} - Appended {}", context.getMember().serverAddress(), entry);
    }

    CompletableFuture<RegisterResponse> future = new CompletableFuture<>();
    appender.appendEntries(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          RegisterEntry entry = context.getLog().get(index);

          LOGGER.debug("{} - Applying {}", context.getMember().serverAddress(), entry);
          context.getStateMachine().apply(entry, true).whenComplete((sessionId, sessionError) -> {
            if (isOpen()) {
              if (sessionError == null) {
                future.complete(logResponse(RegisterResponse.builder()
                  .withStatus(Response.Status.OK)
                  .withSession((Long) sessionId)
                  .withTimeout(timeout)
                  .withLeader(context.getMember().clientAddress())
                  .withMembers(context.buildActiveMembers().stream()
                    .map(Member::clientAddress)
                    .filter(m -> m != null)
                    .collect(Collectors.toList())).build()));
              } else if (sessionError instanceof RaftException) {
                future.complete(logResponse(RegisterResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withError(((RaftException) sessionError).getType())
                  .build()));
              } else {
                future.complete(logResponse(RegisterResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withError(RaftError.Type.INTERNAL_ERROR)
                  .build()));
              }
              checkSessions();
            }
            entry.release();
          });
        } else {
          future.complete(logResponse(RegisterResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.INTERNAL_ERROR)
            .build()));
        }
      }
    });
    return future;
  }

  @Override
  protected CompletableFuture<ConnectResponse> connect(ConnectRequest request, Connection connection) {
    context.checkThread();
    logRequest(request);

    context.getStateMachine().executor().context().sessions().registerConnection(request.session(), connection);

    AcceptRequest acceptRequest = AcceptRequest.builder()
      .withSession(request.session())
      .withAddress(context.getMember().serverAddress())
      .build();
    return accept(acceptRequest)
      .thenApply(acceptResponse -> ConnectResponse.builder().withStatus(Response.Status.OK).build())
      .thenApply(this::logResponse);
  }

  @Override
  protected CompletableFuture<AcceptResponse> accept(AcceptRequest request) {
    final long timestamp = System.currentTimeMillis();
    final long index;

    context.checkThread();
    logRequest(request);

    try (ConnectEntry entry = context.getLog().create(ConnectEntry.class)) {
      entry.setTerm(context.getTerm())
        .setSession(request.session())
        .setTimestamp(timestamp)
        .setAddress(request.address());
      index = context.getLog().append(entry);
      LOGGER.debug("{} - Appended {}", context.getMember().serverAddress(), entry);
    }

    context.getStateMachine().executor().context().sessions().registerAddress(request.session(), request.address());

    CompletableFuture<AcceptResponse> future = new CompletableFuture<>();
    appender.appendEntries(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          ConnectEntry entry = context.getLog().get(index);
          applyEntry(entry).whenComplete((connectResult, connectError) -> {
            if (isOpen()) {
              if (connectError == null) {
                future.complete(logResponse(AcceptResponse.builder()
                  .withStatus(Response.Status.OK)
                  .build()));
              } else if (connectError instanceof RaftException) {
                future.complete(logResponse(AcceptResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withError(((RaftException) connectError).getType())
                  .build()));
              } else {
                future.complete(logResponse(AcceptResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withError(RaftError.Type.INTERNAL_ERROR)
                  .build()));
              }
              checkSessions();
            }
            entry.release();
          });
        } else {
          future.complete(logResponse(AcceptResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.INTERNAL_ERROR)
            .build()));
        }
      }
    });
    return future;
  }

  @Override
  protected CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    final long timestamp = System.currentTimeMillis();
    final long index;

    context.checkThread();
    logRequest(request);

    try (KeepAliveEntry entry = context.getLog().create(KeepAliveEntry.class)) {
      entry.setTerm(context.getTerm())
        .setSession(request.session())
        .setCommandSequence(request.commandSequence())
        .setEventVersion(request.eventVersion())
        .setTimestamp(timestamp);
      index = context.getLog().append(entry);
      LOGGER.debug("{} - Appended {}", context.getMember().serverAddress(), entry);
    }

    CompletableFuture<KeepAliveResponse> future = new CompletableFuture<>();
    appender.appendEntries(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          KeepAliveEntry entry = context.getLog().get(index);
          applyEntry(entry).whenCompleteAsync((sessionResult, sessionError) -> {
            if (isOpen()) {
              if (sessionError == null) {
                future.complete(logResponse(KeepAliveResponse.builder()
                  .withStatus(Response.Status.OK)
                  .withLeader(context.getMember().clientAddress())
                  .withMembers(context.buildActiveMembers().stream()
                    .map(Member::clientAddress)
                    .filter(m -> m != null)
                    .collect(Collectors.toList())).build()));
              } else if (sessionError instanceof RaftException) {
                future.complete(logResponse(KeepAliveResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withLeader(context.getMember().clientAddress())
                  .withError(((RaftException) sessionError).getType())
                  .build()));
              } else {
                future.complete(logResponse(KeepAliveResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withLeader(context.getMember().clientAddress())
                  .withError(RaftError.Type.INTERNAL_ERROR)
                  .build()));
              }
              checkSessions();
            }
            entry.release();
          }, context.getThreadContext().executor());
        } else {
          future.complete(logResponse(KeepAliveResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withLeader(context.getMember().clientAddress())
            .withError(RaftError.Type.INTERNAL_ERROR)
            .build()));
        }
      }
    });
    return future;
  }

  @Override
  protected CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request) {
    final long timestamp = System.currentTimeMillis();
    final long index;

    context.checkThread();
    logRequest(request);

    try (UnregisterEntry entry = context.getLog().create(UnregisterEntry.class)) {
      entry.setTerm(context.getTerm())
        .setSession(request.session())
        .setExpired(false)
        .setTimestamp(timestamp);
      index = context.getLog().append(entry);
      LOGGER.debug("{} - Appended {}", context.getMember().serverAddress(), entry);
    }

    CompletableFuture<UnregisterResponse> future = new CompletableFuture<>();
    appender.appendEntries(index).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          UnregisterEntry entry = context.getLog().get(index);

          LOGGER.debug("{} - Applying {}", context.getMember().serverAddress(), entry);
          context.getStateMachine().apply(entry, true).whenComplete((unregisterResult, unregisterError) -> {
            if (isOpen()) {
              if (unregisterError == null) {
                future.complete(logResponse(UnregisterResponse.builder()
                  .withStatus(Response.Status.OK)
                  .build()));
              } else if (unregisterError instanceof RaftException) {
                future.complete(logResponse(UnregisterResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withError(((RaftException) unregisterError).getType())
                  .build()));
              } else {
                future.complete(logResponse(UnregisterResponse.builder()
                  .withStatus(Response.Status.ERROR)
                  .withError(RaftError.Type.INTERNAL_ERROR)
                  .build()));
              }
              checkSessions();
            }
            entry.release();
          });
        } else {
          future.complete(logResponse(UnregisterResponse.builder()
            .withStatus(Response.Status.ERROR)
            .withError(RaftError.Type.INTERNAL_ERROR)
            .build()));
        }
      }
    });
    return future;
  }

  /**
   * Cancels the append timer.
   */
  private void cancelAppendTimer() {
    if (appendTimer != null) {
      LOGGER.debug("{} - Cancelling append timer", context.getMember().serverAddress());
      appendTimer.cancel();
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close().thenRun(appender::close).thenRun(this::cancelAppendTimer);
  }

}
