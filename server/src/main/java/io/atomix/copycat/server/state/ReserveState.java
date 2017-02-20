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
package io.atomix.copycat.server.state;

import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.protocol.request.*;
import io.atomix.copycat.server.protocol.response.*;

import java.util.concurrent.CompletableFuture;

/**
 * The reserve state receives configuration changes from followers and proxies other requests
 * to active members of the cluster.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ReserveState extends InactiveState {

  public ReserveState(ServerContext context) {
    super(context);
  }

  @Override
  public CopycatServer.State type() {
    return CopycatServer.State.RESERVE;
  }

  @Override
  public CompletableFuture<ServerState> open() {
    return super.open().thenRun(() -> {
      if (type() == CopycatServer.State.RESERVE) {
        context.reset();
      }
    }).thenApply(v -> this);
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(AppendRequest request) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());

    // Update the local commitIndex and globalIndex.
    context.setCommitIndex(request.commitIndex());
    context.setGlobalIndex(request.globalIndex());

    return CompletableFuture.completedFuture(logResponse(AppendResponse.builder()
      .withStatus(ProtocolResponse.Status.OK)
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .withLogIndex(0)
      .build()));
  }

  @Override
  public CompletableFuture<PollResponse> onPoll(PollRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
      .withStatus(ProtocolResponse.Status.ERROR)
      .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> onVote(VoteRequest request) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), 0);

    return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
      .withStatus(ProtocolResponse.Status.ERROR)
      .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<CommandResponse> onCommand(CommandRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(
        logResponse(
          CommandResponse.builder()
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build()));
    } else {
      return forward(connection ->
        connection.command(CommandRequest.builder()
          .withSession(request.session())
          .withSequence(request.sequence())
          .withCommand(request.bytes())
          .build()))
        .thenApply(response ->
          CommandResponse.builder()
            .withStatus(response.status())
            .withError(response.error())
            .withIndex(response.index())
            .withEventIndex(response.eventIndex())
            .withResult(response.result())
            .build())
        .exceptionally(error ->
          CommandResponse.builder()
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<QueryResponse> onQuery(QueryRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(
        logResponse(
          QueryResponse.builder()
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build()));
    } else {
      return forward(connection ->
        connection.query(QueryRequest.builder()
          .withSession(request.session())
          .withSequence(request.sequence())
          .withIndex(request.index())
          .withQuery(request.bytes())
          .withConsistency(request.consistency())
          .build()))
        .thenApply(response ->
          QueryResponse.builder()
            .withStatus(response.status())
            .withError(response.error())
            .withIndex(response.index())
            .withEventIndex(response.eventIndex())
            .withResult(response.result())
            .build())
        .exceptionally(error ->
          QueryResponse.builder()
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<RegisterResponse> onRegister(RegisterRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(
        RegisterResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(connection ->
        connection.register(RegisterRequest.builder()
          .withClient(request.client())
          .withTimeout(request.timeout())
          .build()))
        .thenApply(response ->
          RegisterResponse.builder()
            .withStatus(response.status())
            .withError(response.error())
            .withSession(response.session())
            .withLeader(response.leader())
            .withMembers(response.members())
            .withTimeout(response.timeout())
            .build())
        .exceptionally(error ->
          RegisterResponse.builder()
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<ConnectResponse> onConnect(ConnectRequest request, ProtocolServerConnection connection) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(
      ConnectResponse.builder()
        .withStatus(ProtocolResponse.Status.ERROR)
        .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
        .build()));
  }

  @Override
  public CompletableFuture<AcceptResponse> onAccept(AcceptRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(
      AcceptResponse.builder()
        .withStatus(ProtocolResponse.Status.ERROR)
        .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
        .build()));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> onKeepAlive(KeepAliveRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(
        KeepAliveResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(connection ->
        connection.keepAlive(KeepAliveRequest.builder()
          .withSession(request.session())
          .withCommandSequence(request.commandSequence())
          .withEventIndex(request.commandSequence())
          .build()))
        .thenApply(response ->
          KeepAliveResponse.builder()
            .withStatus(response.status())
            .withError(response.error())
            .withLeader(response.leader())
            .withMembers(response.members())
            .build())
        .exceptionally(error ->
          KeepAliveResponse.builder()
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<PublishResponse> onPublish(PublishRequest request) {
    context.checkThread();
    logRequest(request);

    ServerSession session = context.getStateMachine().context().sessions().getSession(request.session());
    if (session == null || session.getConnection() == null) {
      return CompletableFuture.completedFuture(logResponse(
        PublishResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
          .build()));
    } else {
      CompletableFuture<PublishResponse> future = new CompletableFuture<>();
      session.getConnection().publish(PublishRequest.builder()
        .withSession(request.session())
        .withEventIndex(request.eventIndex())
        .withPreviousIndex(request.previousIndex())
        .withEvents(request.events())
        .build())
        .whenComplete((result, error) -> {
          if (isOpen()) {
            if (error == null) {
              future.complete(result);
            } else {
              future.complete(logResponse(
                PublishResponse.builder()
                  .withStatus(ProtocolResponse.Status.ERROR)
                  .withError(ProtocolResponse.Error.Type.INTERNAL_ERROR)
                  .build()));
            }
          }
        });
      return future;
    }
  }

  @Override
  public CompletableFuture<UnregisterResponse> onUnregister(UnregisterRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(
        UnregisterResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(connection -> connection.unregister(
        UnregisterRequest.builder()
          .withSession(request.session())
          .build()))
        .thenApply(response ->
          UnregisterResponse.builder()
            .withStatus(response.status())
            .withError(response.error())
            .build())
        .exceptionally(error ->
          UnregisterResponse.builder()
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<JoinResponse> onJoin(JoinRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(
        logResponse(
          JoinResponse.builder()
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build()));
    } else {
      return forward(connection -> connection.join(JoinRequest.builder()
        .withMember(request.member())
        .build()))
        .thenApply(response -> {
          if (response.status() == ProtocolResponse.Status.OK) {
            return JoinResponse.builder()
              .withStatus(ProtocolResponse.Status.OK)
              .withIndex(response.index())
              .withTerm(response.term())
              .withMembers(response.members())
              .withTime(response.timestamp())
              .build();
          } else {
            return JoinResponse.builder()
              .withStatus(ProtocolResponse.Status.ERROR)
              .withError(response.error())
              .build();
          }
        }).exceptionally(error ->
          JoinResponse.builder()
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<ReconfigureResponse> onReconfigure(ReconfigureRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(
        ReconfigureResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(connection -> connection.reconfigure(ReconfigureRequest.builder()
        .withIndex(request.index())
        .withTerm(request.term())
        .withMember(request.member())
        .build()))
        .thenApply(response -> {
          if (response.status() == ProtocolResponse.Status.OK) {
            return ReconfigureResponse.builder()
              .withStatus(ProtocolResponse.Status.OK)
              .withIndex(response.index())
              .withTerm(response.term())
              .withMembers(response.members())
              .withTime(response.timestamp())
              .build();
          } else {
            return ReconfigureResponse.builder()
              .withStatus(ProtocolResponse.Status.ERROR)
              .withError(response.error())
              .build();
          }
        }).exceptionally(error ->
          ReconfigureResponse.builder()
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<LeaveResponse> onLeave(LeaveRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(
        LeaveResponse.builder()
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(connection -> connection.leave(LeaveRequest.builder()
        .withMember(request.member())
        .build()))
        .thenApply(response -> {
          if (response.status() == ProtocolResponse.Status.OK) {
            return LeaveResponse.builder()
              .withStatus(ProtocolResponse.Status.OK)
              .withIndex(response.index())
              .withTerm(response.term())
              .withMembers(response.members())
              .withTime(response.timestamp())
              .build();
          } else {
            return LeaveResponse.builder()
              .withStatus(ProtocolResponse.Status.ERROR)
              .withError(response.error())
              .build();
          }
        }).exceptionally(error ->
          LeaveResponse.builder()
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<InstallResponse> onInstall(InstallRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(
      InstallResponse.builder()
        .withStatus(ProtocolResponse.Status.ERROR)
        .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
        .build()));
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenRun(() -> {
      if (type() == CopycatServer.State.RESERVE) {
        context.reset();
      }
    });
  }

}
