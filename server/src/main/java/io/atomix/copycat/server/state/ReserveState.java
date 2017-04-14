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

import io.atomix.catalyst.transport.Connection;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.protocol.*;

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
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());

    // Update the local commitIndex and globalIndex.
    context.setCommitIndex(request.commitIndex());
    context.setGlobalIndex(request.globalIndex());

    return CompletableFuture.completedFuture(logResponse(AppendResponse.builder()
      .withStatus(Response.Status.OK)
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .withLogIndex(0)
      .build()));
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(CopycatError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<VoteResponse> vote(VoteRequest request) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), 0);

    return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(CopycatError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<CommandResponse> command(CommandRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(CopycatError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<CommandRequest, CommandResponse>forward(request)
        .exceptionally(error -> CommandResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(CopycatError.Type.NO_LEADER_ERROR)
          .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(CopycatError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<QueryRequest, QueryResponse>forward(request)
        .exceptionally(error -> QueryResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(CopycatError.Type.NO_LEADER_ERROR)
          .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(RegisterResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(CopycatError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<RegisterRequest, RegisterResponse>forward(request)
        .exceptionally(error -> RegisterResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(CopycatError.Type.NO_LEADER_ERROR)
          .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<ConnectResponse> connect(ConnectRequest request, Connection connection) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(ConnectResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(CopycatError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(KeepAliveResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(CopycatError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<KeepAliveRequest, KeepAliveResponse>forward(request)
        .exceptionally(error -> KeepAliveResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(CopycatError.Type.NO_LEADER_ERROR)
          .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(UnregisterResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(CopycatError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<UnregisterRequest, UnregisterResponse>forward(request)
        .exceptionally(error -> UnregisterResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(CopycatError.Type.NO_LEADER_ERROR)
          .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<JoinResponse> join(JoinRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(CopycatError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<JoinRequest, JoinResponse>forward(request)
        .exceptionally(error -> JoinResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(CopycatError.Type.NO_LEADER_ERROR)
          .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(ReconfigureRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(CopycatError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<ReconfigureRequest, ReconfigureResponse>forward(request)
        .exceptionally(error -> ReconfigureResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(CopycatError.Type.NO_LEADER_ERROR)
          .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(CopycatError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<LeaveRequest, LeaveResponse>forward(request)
        .exceptionally(error -> LeaveResponse.builder()
          .withStatus(Response.Status.ERROR)
          .withError(CopycatError.Type.NO_LEADER_ERROR)
          .build())
        .thenApply(this::logResponse);
    }
  }

  @Override
  public CompletableFuture<InstallResponse> install(InstallRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(InstallResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(CopycatError.Type.ILLEGAL_MEMBER_STATE_ERROR)
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
