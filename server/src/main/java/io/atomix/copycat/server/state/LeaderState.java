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

import io.atomix.copycat.ConsistencyLevel;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.protocol.error.ProtocolException;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.request.*;
import io.atomix.copycat.server.protocol.response.*;
import io.atomix.copycat.server.session.Session;
import io.atomix.copycat.server.storage.Indexed;
import io.atomix.copycat.server.storage.LogWriter;
import io.atomix.copycat.server.storage.entry.*;
import io.atomix.copycat.server.storage.system.Configuration;
import io.atomix.copycat.util.concurrent.ComposableFuture;
import io.atomix.copycat.util.concurrent.Scheduled;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

/**
 * Leader state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
final class LeaderState extends ActiveState {
  // Max request queue size *per session* - necessary to limit the stack size
  private static final int MAX_REQUEST_QUEUE_SIZE = 100;

  private final LeaderAppender appender;
  private Scheduled appendTimer;
  private long configuring;

  public LeaderState(ServerContext context) {
    super(context);
    this.appender = new LeaderAppender(this);
  }

  @Override
  public CopycatServer.State type() {
    return CopycatServer.State.LEADER;
  }

  @Override
  public synchronized CompletableFuture<ServerState> open() {
    // Reset state for the leader.
    takeLeadership();

    // Append initial entries to the log, including an initial no-op entry and the server's configuration.
    appendInitialEntries();

    // Commit the initial leader entries.
    commitInitialEntries();

    return super.open()
      .thenRun(this::startAppendTimer)
      .thenApply(v -> this);
  }

  /**
   * Sets the current node as the cluster leader.
   */
  private void takeLeadership() {
    context.setLeader(context.getCluster().member().id());
    context.getClusterState().getRemoteMemberStates().forEach(m -> m.resetState(context.getLog()));
  }

  /**
   * Appends initial entries to the log to take leadership.
   */
  private void appendInitialEntries() {
    final long term = context.getTerm();

    final LogWriter writer = context.getLogWriter();
    try {
      writer.lock();
      Indexed<InitializeEntry> indexed = writer.append(term, new InitializeEntry(appender.time()));
      LOGGER.debug("{} - Appended {}", context.getCluster().member().address(), indexed.index());
    } finally {
      writer.unlock();
    }

    // Append a configuration entry to propagate the leader's cluster configuration.
    configure(context.getCluster().members());
  }

  /**
   * Commits a no-op entry to the log, ensuring any entries from a previous term are committed.
   */
  private CompletableFuture<Void> commitInitialEntries() {
    // The Raft protocol dictates that leaders cannot commit entries from previous terms until
    // at least one entry from their current term has been stored on a majority of servers. Thus,
    // we force entries to be appended up to the leader's no-op entry. The LeaderAppender will ensure
    // that the commitIndex is not increased until the no-op entry (appender.index()) is committed.
    CompletableFuture<Void> future = new CompletableFuture<>();
    appender.appendEntries(appender.index()).whenComplete((resultIndex, error) -> {
      if (isOpen()) {
        if (error == null) {
          context.getStateMachine().apply(resultIndex);
          future.complete(null);
        } else {
          context.setLeader(0);
          context.transition(CopycatServer.State.FOLLOWER);
        }
      }
    });
    return future;
  }

  /**
   * Starts sending AppendEntries requests to all cluster members.
   */
  private void startAppendTimer() {
    // Set a timer that will be used to periodically synchronize with other nodes
    // in the cluster. This timer acts as a heartbeat to ensure this node remains
    // the leader.
    LOGGER.debug("{} - Starting append timer", context.getCluster().member().address());
    appendTimer = context.getThreadContext().schedule(Duration.ZERO, context.getHeartbeatInterval(), this::appendMembers);
  }

  /**
   * Sends AppendEntries requests to members of the cluster that haven't heard from the leader in a while.
   */
  private void appendMembers() {
    context.checkThread();
    if (isOpen()) {
      appender.appendEntries();
    }
  }

  /**
   * Checks to determine whether any sessions have expired.
   * <p>
   * Copycat allows only leaders to explicitly unregister sessions due to expiration. This ensures
   * that sessions cannot be expired by lengthy election periods or other disruptions to time.
   * To do so, the leader periodically iterates through registered sessions and checks for sessions
   * that have been marked suspicious. The internal state machine marks sessions as suspicious when
   * keep alive entries are not committed for longer than the session timeout. Once the leader marks
   * a session as suspicious, it will log and replicate an {@link UnregisterEntry} to unregister the session.
   */
  private void checkSessions() {
    long term = context.getTerm();

    // Iterate through all currently registered sessions.
    for (ServerSession session : context.getStateMachine().context().sessions().sessions.values()) {
      // If the session isn't already being unregistered by this leader and a keep-alive entry hasn't
      // been committed for the session in some time, log and commit a new UnregisterEntry.
      if (session.state() == Session.State.UNSTABLE && !session.isUnregistering()) {
        LOGGER.debug("{} - Detected expired session: {}", context.getCluster().member().address(), session.id());

        // Log the unregister entry, indicating that the session was explicitly unregistered by the leader.
        // This will result in state machine expire() methods being called when the entry is applied.
        final LogWriter writer = context.getLogWriter();
        final Indexed<UnregisterEntry> entry;
        try {
          writer.lock();
          entry = writer.append(term, new UnregisterEntry(System.currentTimeMillis(), session.id(), true));
          LOGGER.debug("{} - Appended {}", context.getCluster().member().address(), entry);
        } finally {
          writer.unlock();
        }

        // Commit the unregister entry and apply it to the state machine.
        appender.appendEntries(entry.index()).whenComplete((result, error) -> {
          if (isOpen()) {
            context.getStateMachine().apply(entry.index());
          }
        });

        // Mark the session as being unregistered in order to ensure this leader doesn't attempt
        // to unregister it again.
        session.unregister();
      }
    }
  }

  /**
   * Returns a boolean value indicating whether a configuration is currently being committed.
   *
   * @return Indicates whether a configuration is currently being committed.
   */
  boolean configuring() {
    return configuring > 0;
  }

  /**
   * Returns a boolean value indicating whether the leader is still being initialized.
   *
   * @return Indicates whether the leader is still being initialized.
   */
  boolean initializing() {
    // If the leader index is 0 or is greater than the commitIndex, do not allow configuration changes.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    return appender.index() == 0 || context.getCommitIndex() < appender.index();
  }

  /**
   * Commits the given configuration.
   */
  protected CompletableFuture<Long> configure(Collection<Member> members) {
    final long term = context.getTerm();

    final LogWriter writer = context.getLogWriter();
    final Indexed<ConfigurationEntry> entry;
    try {
      writer.lock();
      entry = writer.append(term, new ConfigurationEntry(System.currentTimeMillis(), members));
      LOGGER.debug("{} - Appended {}", context.getCluster().member().address(), entry);
    } finally {
      writer.unlock();
    }

    context.getClusterState().configure(new Configuration(entry.index(), entry.term(), entry.entry().timestamp(), entry.entry().members()));

    return appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        // Reset the configuration index to allow new configuration changes to be committed.
        configuring = 0;
      }
    });
  }

  @Override
  public CompletableFuture<JoinResponse> onJoin(final JoinRequest request) {
    context.checkThread();
    logRequest(request);

    // If another configuration change is already under way, reject the configuration.
    // If the leader index is 0 or is greater than the commitIndex, reject the join requests.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    if (configuring() || initializing()) {
      return CompletableFuture.completedFuture(logResponse(
        JoinResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .build()));
    }

    // If the member is already a known member of the cluster, complete the join successfully.
    if (context.getCluster().member(request.member().id()) != null) {
      return CompletableFuture.completedFuture(logResponse(
        JoinResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withIndex(context.getClusterState().getConfiguration().index())
          .withTerm(context.getClusterState().getConfiguration().term())
          .withTime(context.getClusterState().getConfiguration().time())
          .withMembers(context.getCluster().members())
          .build()));
    }

    Member member = request.member();

    // Add the joining member to the members list. If the joining member's type is ACTIVE, join the member in the
    // PROMOTABLE state to allow it to get caught up without impacting the quorum size.
    Collection<Member> members = context.getCluster().members();
    members.add(new ServerMember(member.type(), member.serverAddress(), member.clientAddress(), Instant.now()));

    CompletableFuture<JoinResponse> future = new CompletableFuture<>();
    configure(members).whenComplete((index, error) -> {
      context.checkThread();
      if (isOpen()) {
        if (error == null) {
          future.complete(logResponse(
            JoinResponse.builder()
              .withStatus(ProtocolResponse.Status.OK)
              .withIndex(index)
              .withTerm(context.getClusterState().getConfiguration().term())
              .withTime(context.getClusterState().getConfiguration().time())
              .withMembers(members)
              .build()));
        } else {
          future.complete(logResponse(
            JoinResponse.builder()
              .withStatus(ProtocolResponse.Status.ERROR)
              .withError(PollResponse.Error.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<ReconfigureResponse> onReconfigure(final ReconfigureRequest request) {
    context.checkThread();
    logRequest(request);

    // If another configuration change is already under way, reject the configuration.
    // If the leader index is 0 or is greater than the commitIndex, reject the promote requests.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    if (configuring() || initializing()) {
      return CompletableFuture.completedFuture(logResponse(
        ReconfigureResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ReconfigureResponse.Error.Type.CONFIGURATION_ERROR)
          .build()));
    }

    // If the member is not a known member of the cluster, fail the promotion.
    ServerMember existingMember = context.getClusterState().member(request.member().id());
    if (existingMember == null) {
      return CompletableFuture.completedFuture(logResponse(
        ReconfigureResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(PollResponse.Error.Type.UNKNOWN_SESSION_ERROR)
          .build()));
    }

    // If the configuration request index is less than the last known configuration index for
    // the leader, fail the request to ensure servers can't reconfigure an old configuration.
    if (request.index() > 0 && request.index() < context.getClusterState().getConfiguration().index() || request.term() != context.getClusterState().getConfiguration().term()
      && (existingMember.type() != request.member().type() || existingMember.status() != request.member().status())) {
      return CompletableFuture.completedFuture(logResponse(
        ReconfigureResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(PollResponse.Error.Type.CONFIGURATION_ERROR)
          .build()));
    }

    Member member = request.member();

    // If the client address is being set or has changed, update the configuration.
    if (member.clientAddress() != null && (existingMember.clientAddress() == null || !existingMember.clientAddress().equals(member.clientAddress()))) {
      existingMember.update(member.clientAddress(), Instant.now());
    }

    // Update the member type.
    existingMember.update(request.member().type(), Instant.now());

    Collection<Member> members = context.getCluster().members();

    CompletableFuture<ReconfigureResponse> future = new CompletableFuture<>();
    configure(members).whenComplete((index, error) -> {
      context.checkThread();
      if (isOpen()) {
        if (error == null) {
          future.complete(logResponse(
            ReconfigureResponse.builder()
              .withStatus(ProtocolResponse.Status.OK)
              .withIndex(index)
              .withTerm(context.getClusterState().getConfiguration().term())
              .withTime(context.getClusterState().getConfiguration().time())
              .withMembers(members)
              .build()));
        } else {
          future.complete(logResponse(
            ReconfigureResponse.builder()
              .withStatus(ProtocolResponse.Status.ERROR)
              .withError(PollResponse.Error.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<LeaveResponse> onLeave(final LeaveRequest request) {
    context.checkThread();
    logRequest(request);

    // If another configuration change is already under way, reject the configuration.
    // If the leader index is 0 or is greater than the commitIndex, reject the join requests.
    // Configuration changes should not be allowed until the leader has committed a no-op entry.
    // See https://groups.google.com/forum/#!topic/raft-dev/t4xj6dJTP6E
    if (configuring() || initializing()) {
      return CompletableFuture.completedFuture(logResponse(
        LeaveResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .build()));
    }

    // If the leaving member is not a known member of the cluster, complete the leave successfully.
    if (context.getCluster().member(request.member().id()) == null) {
      return CompletableFuture.completedFuture(logResponse(
        LeaveResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withMembers(context.getCluster().members())
          .build()));
    }

    Member member = request.member();

    Collection<Member> members = context.getCluster().members();
    members.remove(member);

    CompletableFuture<LeaveResponse> future = new CompletableFuture<>();
    configure(members).whenComplete((index, error) -> {
      context.checkThread();
      if (isOpen()) {
        if (error == null) {
          future.complete(logResponse(
            LeaveResponse.builder()
              .withStatus(ProtocolResponse.Status.OK)
              .withIndex(index)
              .withTerm(context.getClusterState().getConfiguration().term())
              .withTime(context.getClusterState().getConfiguration().time())
              .withMembers(members)
              .build()));
        } else {
          future.complete(logResponse(
            LeaveResponse.builder()
              .withStatus(ProtocolResponse.Status.ERROR)
              .withError(PollResponse.Error.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<PollResponse> onPoll(final PollRequest request) {
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(
      PollResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withTerm(context.getTerm())
        .withAccepted(false)
        .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> onVote(final VoteRequest request) {
    if (updateTermAndLeader(request.term(), 0)) {
      LOGGER.debug("{} - Received greater term", context.getCluster().member().address());
      context.transition(CopycatServer.State.FOLLOWER);
      return super.onVote(request);
    } else {
      logRequest(request);
      return CompletableFuture.completedFuture(logResponse(
        VoteResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withTerm(context.getTerm())
          .withVoted(false)
          .build()));
    }
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(final AppendRequest request) {
    context.checkThread();
    if (updateTermAndLeader(request.term(), request.leader())) {
      CompletableFuture<AppendResponse> future = super.onAppend(request);
      context.transition(CopycatServer.State.FOLLOWER);
      return future;
    } else if (request.term() < context.getTerm()) {
      logRequest(request);
      return CompletableFuture.completedFuture(logResponse(
        AppendResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withTerm(context.getTerm())
          .withSucceeded(false)
          .withLogIndex(context.getLogWriter().lastIndex())
          .build()));
    } else {
      context.setLeader(request.leader()).transition(CopycatServer.State.FOLLOWER);
      return super.onAppend(request);
    }
  }

  @Override
  public CompletableFuture<CommandResponse> onCommand(final CommandRequest request) {
    context.checkThread();
    logRequest(request);

    // Get the client's server session. If the session doesn't exist, return an unknown session error.
    ServerSession session = context.getStateMachine().context().sessions().getSession(request.session());
    if (session == null) {
      return CompletableFuture.completedFuture(logResponse(
        CommandResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(PollResponse.Error.Type.UNKNOWN_SESSION_ERROR)
          .build()));
    }

    ComposableFuture<CommandResponse> future = new ComposableFuture<>();
    sequenceCommand(request, session, future);
    return future;
  }

  /**
   * Sequences the given command to the log.
   */
  private void sequenceCommand(CommandRequest request, ServerSession session, CompletableFuture<CommandResponse> future) {
    // If the command is LINEARIZABLE and the session's current sequence number is less then one prior to the request
    // sequence number, queue this request for handling later. We want to handle command requests in the order in which
    // they were sent by the client. Note that it's possible for the session sequence number to be greater than the request
    // sequence number. In that case, it's likely that the command was submitted more than once to the
    // cluster, and the command will be deduplicated once applied to the state machine.
    if (request.sequence() > session.nextRequestSequence()) {
      // If the request sequence number is more than 1k requests above the last sequenced request, reject the request.
      // The client should resubmit a request that fails with a COMMAND_ERROR.
      if (request.sequence() - session.getRequestSequence() > MAX_REQUEST_QUEUE_SIZE) {
        future.complete(CommandResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(PollResponse.Error.Type.COMMAND_ERROR)
          .build());
      }
      // Register the request in the request queue if it's not too far ahead of the current sequence number.
      else {
        session.registerRequest(request.sequence(), () -> applyCommand(request, session, future));
      }
    } else {
      applyCommand(request, session, future);
    }
  }

  /**
   * Applies the given command to the log.
   */
  private void applyCommand(CommandRequest request, ServerSession session, CompletableFuture<CommandResponse> future) {
    final long term = context.getTerm();
    final long timestamp = System.currentTimeMillis();
    final Indexed<CommandEntry> entry;

    final LogWriter writer = context.getLogWriter();
    try {
      writer.lock();
      entry = writer.append(term, new CommandEntry(timestamp, request.session(), request.sequence(), request.bytes()));
      LOGGER.debug("{} - Appended {}", context.getCluster().member().address(), entry);
    } finally {
      writer.unlock();
    }

    // Replicate the command to followers.
    appendCommand(entry.index(), future);

    // Set the last processed request for the session. This will cause sequential command callbacks to be executed.
    session.setRequestSequence(request.sequence());
  }

  /**
   * Sends append requests for a command to followers.
   */
  private void appendCommand(long index, CompletableFuture<CommandResponse> future) {
    appender.appendEntries(index).whenComplete((commitIndex, commitError) -> {
      if (isOpen()) {
        if (commitError == null) {
          applyCommand(index, future);
        } else {
          future.complete(logResponse(
            CommandResponse.builder()
              .withStatus(ProtocolResponse.Status.ERROR)
              .withError(PollResponse.Error.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });
  }

  /**
   * Applies a command to the state machine.
   */
  private void applyCommand(long index, CompletableFuture<CommandResponse> future) {
    context.getStateMachine().<ServerStateMachine.Result>apply(index).whenComplete((result, error) -> {
      if (isOpen()) {
        completeOperation(result, CommandResponse.builder(), error, future);
      }
    });
  }

  @Override
  public CompletableFuture<QueryResponse> onQuery(final QueryRequest request) {
    final long timestamp = System.currentTimeMillis();

    context.checkThread();
    logRequest(request);

    Indexed<QueryEntry> entry = new Indexed<>(
      request.index(),
      context.getTerm(),
      new QueryEntry(
        timestamp,
        request.session(),
        request.sequence(),
        request.bytes()),
      0);

    ConsistencyLevel consistency = request.consistency();
    if (consistency == null) {
      return queryLinearizable(entry);
    }

    switch (consistency) {
      case SEQUENTIAL:
        return queryLocal(entry);
      case LINEARIZABLE_LEASE:
        return queryBoundedLinearizable(entry);
      case LINEARIZABLE:
        return queryLinearizable(entry);
      default:
        throw new IllegalStateException("unknown consistency level");
    }
  }

  /**
   * Submits a query with lease bounded linearizable consistency.
   */
  private CompletableFuture<QueryResponse> queryBoundedLinearizable(Indexed<QueryEntry> entry) {
    // Get the client's server session. If the session doesn't exist, return an unknown session error.
    ServerSession session = context.getStateMachine().context().sessions().getSession(entry.entry().session());
    if (session == null) {
      return CompletableFuture.completedFuture(logResponse(
        QueryResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(PollResponse.Error.Type.UNKNOWN_SESSION_ERROR)
          .build()));
    }

    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    sequenceBoundedLinearizableQuery(entry, session, future);
    return future;
  }

  /**
   * Sequences a bounded linearizable query.
   */
  private void sequenceBoundedLinearizableQuery(Indexed<QueryEntry> entry, ServerSession session, CompletableFuture<QueryResponse> future) {
    // If the query's sequence number is greater than the session's current sequence number, queue the request for
    // handling once the state machine is caught up.
    if (entry.entry().sequence() > session.getCommandSequence()) {
      session.registerSequenceQuery(entry.entry().sequence(), () -> applyQuery(entry, future));
    } else {
      applyQuery(entry, future);
    }
  }

  /**
   * Submits a query with strict linearizable consistency.
   */
  private CompletableFuture<QueryResponse> queryLinearizable(Indexed<QueryEntry> entry) {
    // Get the client's server session. If the session doesn't exist, return an unknown session error.
    ServerSession session = context.getStateMachine().context().sessions().getSession(entry.entry().session());
    if (session == null) {
      return CompletableFuture.completedFuture(logResponse(
        QueryResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(PollResponse.Error.Type.UNKNOWN_SESSION_ERROR)
          .build()));
    }

    CompletableFuture<QueryResponse> future = new CompletableFuture<>();
    appendLinearizableQuery(entry, session, future);
    return future;
  }

  /**
   * Sends an append request for the given query entry.
   */
  private void appendLinearizableQuery(Indexed<QueryEntry> entry, ServerSession session, CompletableFuture<QueryResponse> future) {
    appender.appendEntries().whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          sequenceLinearizableQuery(entry, future);
        } else {
          future.complete(logResponse(
            QueryResponse.builder()
              .withStatus(ProtocolResponse.Status.ERROR)
              .withError(PollResponse.Error.Type.QUERY_ERROR)
              .build()));
        }
      }
    });
  }

  /**
   * Sequences a linearizable query.
   */
  private void sequenceLinearizableQuery(Indexed<QueryEntry> entry, CompletableFuture<QueryResponse> future) {
    // Get the client's server session. If the session doesn't exist, return an unknown session error.
    ServerSession session = context.getStateMachine().context().sessions().getSession(entry.entry().session());
    if (session == null) {
      future.complete(logResponse(
        QueryResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(PollResponse.Error.Type.UNKNOWN_SESSION_ERROR)
          .build()));
    } else {
      // If the query's sequence number is greater than the session's current sequence number, queue the request for
      // handling once the state machine is caught up.
      if (entry.entry().sequence() > session.getCommandSequence()) {
        session.registerSequenceQuery(entry.entry().sequence(), () -> applyQuery(entry, future));
      } else {
        applyQuery(entry, future);
      }
    }
  }

  @Override
  public CompletableFuture<RegisterResponse> onRegister(RegisterRequest request) {
    final long term = context.getTerm();
    final long timestamp = System.currentTimeMillis();
    final Indexed<RegisterEntry> entry;

    // If the client submitted a session timeout, use the client's timeout, otherwise use the configured
    // default server session timeout.
    final long timeout;
    if (request.timeout() != 0) {
      timeout = request.timeout();
    } else {
      timeout = context.getSessionTimeout().toMillis();
    }

    context.checkThread();
    logRequest(request);

    final LogWriter writer = context.getLogWriter();
    try {
      writer.lock();
      entry = writer.append(term, new RegisterEntry(timestamp, request.client(), timeout));
      LOGGER.debug("{} - Appended {}", context.getCluster().member().address(), entry);
    } finally {
      writer.unlock();
    }

    CompletableFuture<RegisterResponse> future = new CompletableFuture<>();
    appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          context.getStateMachine().apply(entry.index()).whenComplete((sessionId, sessionError) -> {
            if (isOpen()) {
              if (sessionError == null) {
                future.complete(logResponse(
                  RegisterResponse.builder()
                    .withStatus(ProtocolResponse.Status.OK)
                    .withSession((Long) sessionId)
                    .withTimeout(timeout)
                    .withLeader(context.getCluster().member().clientAddress())
                    .withMembers(context.getCluster().members().stream()
                      .map(Member::clientAddress)
                      .filter(m -> m != null)
                      .collect(Collectors.toList())).build()));
              } else if (sessionError instanceof CompletionException && sessionError.getCause() instanceof ProtocolException) {
                future.complete(logResponse(
                  RegisterResponse.builder()
                    .withStatus(ProtocolResponse.Status.ERROR)
                    .withError(((ProtocolException) sessionError.getCause()).getType())
                    .build()));
              } else if (sessionError instanceof ProtocolException) {
                future.complete(logResponse(
                  RegisterResponse.builder()
                    .withStatus(ProtocolResponse.Status.ERROR)
                    .withError(((ProtocolException) sessionError).getType())
                    .build()));
              } else {
                future.complete(logResponse(
                  RegisterResponse.builder()
                    .withStatus(ProtocolResponse.Status.ERROR)
                    .withError(PollResponse.Error.Type.INTERNAL_ERROR)
                    .build()));
              }
              checkSessions();
            }
          });
        } else {
          future.complete(logResponse(
            RegisterResponse.builder()
              .withStatus(ProtocolResponse.Status.ERROR)
              .withError(PollResponse.Error.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });

    return future;
  }

  @Override
  public CompletableFuture<ConnectResponse> onConnect(ConnectRequest request, ProtocolServerConnection connection) {
    context.checkThread();
    logRequest(request);

    context.getStateMachine().context().sessions().registerConnection(request.client(), connection);

    AcceptRequest acceptRequest = new AcceptRequest.Builder()
      .withClient(request.client())
      .withAddress(context.getCluster().member().serverAddress())
      .build();
    return onAccept(acceptRequest)
      .thenApply(acceptResponse ->
        ConnectResponse.builder()
          .withStatus(ProtocolResponse.Status.OK)
          .withLeader(context.getCluster().member().clientAddress())
          .withMembers(context.getCluster().members().stream()
            .map(Member::clientAddress)
            .filter(m -> m != null)
            .collect(Collectors.toList()))
          .build())
      .thenApply(this::logResponse);
  }

  @Override
  public CompletableFuture<AcceptResponse> onAccept(AcceptRequest request) {
    final long term = context.getTerm();
    final long timestamp = System.currentTimeMillis();
    final Indexed<ConnectEntry> entry;

    context.checkThread();
    logRequest(request);

    final LogWriter writer = context.getLogWriter();
    try {
      writer.lock();
      entry = writer.append(term, new ConnectEntry(timestamp, request.client(), request.address()));
      LOGGER.debug("{} - Appended {}", context.getCluster().member().address(), entry);
    } finally {
      writer.unlock();
    }

    context.getStateMachine().context().sessions().registerAddress(request.client(), request.address());

    CompletableFuture<AcceptResponse> future = new CompletableFuture<>();
    appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          context.getStateMachine().apply(entry.index()).whenComplete((connectResult, connectError) -> {
            if (isOpen()) {
              if (connectError == null) {
                future.complete(logResponse(
                  AcceptResponse.builder()
                    .withStatus(ProtocolResponse.Status.OK)
                    .build()));
              } else if (connectError instanceof CompletionException && connectError.getCause() instanceof ProtocolException) {
                future.complete(logResponse(
                  AcceptResponse.builder()
                    .withStatus(ProtocolResponse.Status.ERROR)
                    .withError(((ProtocolException) connectError.getCause()).getType())
                    .build()));
              } else if (connectError instanceof ProtocolException) {
                future.complete(logResponse(
                  AcceptResponse.builder()
                    .withStatus(ProtocolResponse.Status.ERROR)
                    .withError(((ProtocolException) connectError).getType())
                    .build()));
              } else {
                future.complete(logResponse(
                  AcceptResponse.builder()
                    .withStatus(ProtocolResponse.Status.ERROR)
                    .withError(PollResponse.Error.Type.INTERNAL_ERROR)
                    .build()));
              }
              checkSessions();
            }
          });
        } else {
          future.complete(logResponse(
            AcceptResponse.builder()
              .withStatus(ProtocolResponse.Status.ERROR)
              .withError(PollResponse.Error.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });

    return future;
  }

  @Override
  public CompletableFuture<KeepAliveResponse> onKeepAlive(KeepAliveRequest request) {
    final long term = context.getTerm();
    final long timestamp = System.currentTimeMillis();
    final Indexed<KeepAliveEntry> entry;

    context.checkThread();
    logRequest(request);

    final LogWriter writer = context.getLogWriter();
    try {
      writer.lock();
      entry = writer.append(term, new KeepAliveEntry(timestamp, request.session(), request.commandSequence(), request.eventIndex()));
    } finally {
      writer.unlock();
    }

    CompletableFuture<KeepAliveResponse> future = new CompletableFuture<>();
    appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          context.getStateMachine().apply(entry.index()).whenComplete((sessionResult, sessionError) -> {
            if (isOpen()) {
              if (sessionError == null) {
                future.complete(logResponse(
                  KeepAliveResponse.builder()
                    .withStatus(ProtocolResponse.Status.OK)
                    .withLeader(context.getCluster().member().clientAddress())
                    .withMembers(context.getCluster().members().stream()
                      .map(Member::clientAddress)
                      .filter(m -> m != null)
                      .collect(Collectors.toList())).build()));
              } else if (sessionError instanceof CompletionException && sessionError.getCause() instanceof ProtocolException) {
                future.complete(logResponse(
                  KeepAliveResponse.builder()
                    .withStatus(ProtocolResponse.Status.ERROR)
                    .withLeader(context.getCluster().member().clientAddress())
                    .withError(((ProtocolException) sessionError.getCause()).getType())
                    .build()));
              } else if (sessionError instanceof ProtocolException) {
                future.complete(logResponse(
                  KeepAliveResponse.builder()
                    .withStatus(ProtocolResponse.Status.ERROR)
                    .withLeader(context.getCluster().member().clientAddress())
                    .withError(((ProtocolException) sessionError).getType())
                    .build()));
              } else {
                future.complete(logResponse(
                  KeepAliveResponse.builder()
                    .withStatus(ProtocolResponse.Status.ERROR)
                    .withLeader(context.getCluster().member().clientAddress())
                    .withError(PollResponse.Error.Type.INTERNAL_ERROR)
                    .build()));
              }
              checkSessions();
            }
          });
        } else {
          future.complete(logResponse(
            KeepAliveResponse.builder()
              .withStatus(ProtocolResponse.Status.ERROR)
              .withLeader(context.getCluster().member().clientAddress())
              .withError(PollResponse.Error.Type.INTERNAL_ERROR)
              .build()));
        }
      }
    });

    return future;
  }

  @Override
  public CompletableFuture<UnregisterResponse> onUnregister(UnregisterRequest request) {
    final long term = context.getTerm();
    final long timestamp = System.currentTimeMillis();
    final Indexed<UnregisterEntry> entry;

    context.checkThread();
    logRequest(request);

    final LogWriter writer = context.getLogWriter();
    try {
      writer.lock();
      entry = writer.append(term, new UnregisterEntry(timestamp, request.session(), false));
    } finally {
      writer.unlock();
    }

    CompletableFuture<UnregisterResponse> future = new CompletableFuture<>();
    appender.appendEntries(entry.index()).whenComplete((commitIndex, commitError) -> {
      context.checkThread();
      if (isOpen()) {
        if (commitError == null) {
          context.getStateMachine().apply(entry.index()).whenComplete((unregisterResult, unregisterError) -> {
            if (isOpen()) {
              if (unregisterError == null) {
                future.complete(logResponse(
                  UnregisterResponse.builder()
                    .withStatus(ProtocolResponse.Status.OK)
                    .build()));
              } else if (unregisterError instanceof CompletionException && unregisterError.getCause() instanceof ProtocolException) {
                future.complete(logResponse(
                  UnregisterResponse.builder()
                    .withStatus(ProtocolResponse.Status.ERROR)
                    .withError(((ProtocolException) unregisterError.getCause()).getType())
                    .build()));
              } else if (unregisterError instanceof ProtocolException) {
                future.complete(logResponse(
                  UnregisterResponse.builder()
                    .withStatus(ProtocolResponse.Status.ERROR)
                    .withError(((ProtocolException) unregisterError).getType())
                    .build()));
              } else {
                future.complete(logResponse(
                  UnregisterResponse.builder()
                    .withStatus(ProtocolResponse.Status.ERROR)
                    .withError(PollResponse.Error.Type.INTERNAL_ERROR)
                    .build()));
              }
              checkSessions();
            }
          });
        } else {
          future.complete(logResponse(
            UnregisterResponse.builder()
              .withStatus(ProtocolResponse.Status.ERROR)
              .withError(PollResponse.Error.Type.INTERNAL_ERROR)
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
      LOGGER.debug("{} - Cancelling append timer", context.getCluster().member().address());
      appendTimer.cancel();
    }
  }

  /**
   * Ensures the local server is not the leader.
   */
  private void stepDown() {
    if (context.getLeader() != null && context.getLeader().equals(context.getCluster().member())) {
      context.setLeader(0);
    }
  }

  @Override
  public synchronized CompletableFuture<Void> close() {
    return super.close()
      .thenRun(appender::close)
      .thenRun(this::cancelAppendTimer)
      .thenRun(this::stepDown);
  }

}
