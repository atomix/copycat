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
  public CompletableFuture<AppendResponse> onAppend(AppendRequest request, AppendResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());

    // Update the local commitIndex and globalIndex.
    context.setCommitIndex(request.commitIndex());
    context.setGlobalIndex(request.globalIndex());

    return CompletableFuture.completedFuture(logResponse(responseBuilder
      .withStatus(ProtocolResponse.Status.OK)
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .withLogIndex(0)
      .build()));
  }

  @Override
  public CompletableFuture<PollResponse> onPoll(PollRequest request, PollResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(responseBuilder
      .withStatus(ProtocolResponse.Status.ERROR)
      .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> onVote(VoteRequest request, VoteResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), 0);

    return CompletableFuture.completedFuture(logResponse(responseBuilder
      .withStatus(ProtocolResponse.Status.ERROR)
      .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<CommandResponse> onCommand(CommandRequest request, CommandResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(
        logResponse(
          responseBuilder
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build()));
    } else {
      return forward(connection ->
        connection.command(builder ->
          builder
            .withSession(request.session())
            .withSequence(request.sequence())
            .withCommand(request.command())
            .build()))
        .thenApply(response ->
          responseBuilder
            .withStatus(response.status())
            .withError(response.error().type(), response.error().message())
            .withIndex(response.index())
            .withEventIndex(response.eventIndex())
            .withResult(response.result())
            .build())
        .exceptionally(error ->
          responseBuilder
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<QueryResponse> onQuery(QueryRequest request, QueryResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(
        logResponse(
          responseBuilder
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build()));
    } else {
      return forward(connection ->
        connection.query(builder ->
          builder
            .withSession(request.session())
            .withSequence(request.sequence())
            .withIndex(request.index())
            .withQuery(request.query())
            .build()))
        .thenApply(response ->
          responseBuilder
            .withStatus(response.status())
            .withError(response.error().type(), response.error().message())
            .withIndex(response.index())
            .withEventIndex(response.eventIndex())
            .withResult(response.result())
            .build())
        .exceptionally(error ->
          responseBuilder
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<RegisterResponse> onRegister(RegisterRequest request, RegisterResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(responseBuilder
        .withStatus(ProtocolResponse.Status.ERROR)
        .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return forward(connection ->
        connection.register(builder ->
          builder
            .withClient(request.client())
            .withTimeout(request.timeout())
            .build()))
        .thenApply(response ->
          responseBuilder
            .withStatus(response.status())
            .withError(response.error().type(), response.error().message())
            .withSession(response.session())
            .withLeader(response.leader())
            .withMembers(response.members())
            .withTimeout(response.timeout())
            .build())
        .exceptionally(error ->
          responseBuilder
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<ConnectResponse> onConnect(ConnectRequest request, ConnectResponse.Builder responseBuilder, ProtocolServerConnection connection) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(responseBuilder
      .withStatus(ProtocolResponse.Status.ERROR)
      .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<AcceptResponse> onAccept(AcceptRequest request, AcceptResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(responseBuilder
      .withStatus(ProtocolResponse.Status.ERROR)
      .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> onKeepAlive(KeepAliveRequest request, KeepAliveResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(responseBuilder
        .withStatus(ProtocolResponse.Status.ERROR)
        .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return forward(connection ->
        connection.keepAlive(builder ->
          builder
            .withSession(request.session())
            .withCommandSequence(request.commandSequence())
            .withEventIndex(request.commandSequence())
            .build()))
        .thenApply(response ->
          responseBuilder
            .withStatus(response.status())
            .withError(response.error().type(), response.error().message())
            .withLeader(response.leader())
            .withMembers(response.members())
            .build())
        .exceptionally(error ->
          responseBuilder
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<PublishResponse> onPublish(PublishRequest request, PublishResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);

    ServerSessionContext session = context.getStateMachine().executor().context().sessions().getSession(request.session());
    if (session == null || session.getConnection() == null) {
      return CompletableFuture.completedFuture(logResponse(responseBuilder
        .withStatus(ProtocolResponse.Status.ERROR)
        .withError(ProtocolResponse.Error.Type.ILLEGAL_MEMBER_STATE_ERROR)
        .build()));
    } else {
      CompletableFuture<PublishResponse> future = new CompletableFuture<>();
      session.getConnection().publish(builder ->
        builder
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
                responseBuilder
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
  public CompletableFuture<UnregisterResponse> onUnregister(UnregisterRequest request, UnregisterResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(responseBuilder
        .withStatus(ProtocolResponse.Status.ERROR)
        .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return forward(connection -> connection.unregister(builder ->
        builder
          .withSession(request.session())
          .build()))
        .thenApply(response ->
          responseBuilder
            .withStatus(response.status())
            .withError(response.error().type(), response.error().message())
            .build())
        .exceptionally(error ->
          responseBuilder
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<JoinResponse> onJoin(JoinRequest request, JoinResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(
        logResponse(
          responseBuilder
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build()));
    } else {
      return forward(connection -> connection.join(builder ->
        builder
          .withMember(request.member())
          .build()))
        .thenApply(response ->
          responseBuilder
            .withStatus(response.status())
            .withError(response.error())
            .withIndex(response.index())
            .withTerm(response.term())
            .withMembers(response.members())
            .withTime(response.timestamp())
            .build())
        .exceptionally(error ->
          responseBuilder
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<ReconfigureResponse> onReconfigure(ReconfigureRequest request, ReconfigureResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(
        responseBuilder
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
          .build()));
    } else {
      return forward(connection -> connection.reconfigure(builder ->
        builder
          .withIndex(request.index())
          .withTerm(request.term())
          .withMember(request.member())
          .build()))
        .thenApply(response ->
          responseBuilder
            .withStatus(response.status())
            .withError(response.error().type(), response.error().message())
            .withIndex(response.index())
            .withTerm(response.term())
            .withMembers(response.members())
            .withTime(response.timestamp())
            .build())
        .exceptionally(error ->
          responseBuilder
            .withStatus(ProtocolResponse.Status.ERROR)
            .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
            .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<LeaveResponse> onLeave(LeaveRequest request, LeaveResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(responseBuilder
        .withStatus(ProtocolResponse.Status.ERROR)
        .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return forward(connection -> connection.leave(builder ->
        builder
          .withMember(request.member())
          .build()))
        .thenApply(response ->
          responseBuilder
            .withStatus(response.status())
            .withError(response.error().type(), response.error().message())
            .withIndex(response.index())
            .withTerm(response.term())
            .withMembers(response.members())
            .withTime(response.timestamp())
            .build())
        .exceptionally(error -> responseBuilder
          .withStatus(ProtocolResponse.Status.ERROR)
          .withError(ProtocolResponse.Error.Type.NO_LEADER_ERROR)
          .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<InstallResponse> onInstall(InstallRequest request, InstallResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(responseBuilder
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
