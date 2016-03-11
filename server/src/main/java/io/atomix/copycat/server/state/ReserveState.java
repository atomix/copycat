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
  public CompletableFuture<AbstractState> open() {
    return super.open().thenRun(() -> {
      if (type() == CopycatServer.State.RESERVE) {
        context.reset();
      }
    }).thenApply(v -> this);
  }

  @Override
  protected CompletableFuture<AppendResponse> append(AppendRequest request) {
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
  protected CompletableFuture<PollResponse> poll(PollRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(PollResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(CopycatError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<VoteResponse> vote(VoteRequest request) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), 0);

    return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(CopycatError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<CommandResponse> command(CommandRequest request) {
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
  protected CompletableFuture<QueryResponse> query(QueryRequest request) {
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
  protected CompletableFuture<RegisterResponse> register(RegisterRequest request) {
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
  protected CompletableFuture<ConnectResponse> connect(ConnectRequest request, Connection connection) {
    context.checkThread();
    logRequest(request);
    return CompletableFuture.completedFuture(logResponse(ConnectResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(CopycatError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<AcceptResponse> accept(AcceptRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(AcceptResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(CopycatError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
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
  protected CompletableFuture<PublishResponse> publish(PublishRequest request) {
    context.checkThread();
    logRequest(request);

    ServerSessionContext session = context.getStateMachine().executor().context().sessions().getSession(request.session());
    if (session == null || session.getConnection() == null) {
      return CompletableFuture.completedFuture(logResponse(PublishResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(CopycatError.Type.ILLEGAL_MEMBER_STATE_ERROR)
        .build()));
    } else {
      CompletableFuture<PublishResponse> future = new CompletableFuture<>();
      session.getConnection().<PublishRequest, PublishResponse>send(request).whenComplete((result, error) -> {
        if (isOpen()) {
          if (error == null) {
            future.complete(result);
          } else {
            future.complete(logResponse(PublishResponse.builder()
              .withStatus(Response.Status.ERROR)
              .withError(CopycatError.Type.INTERNAL_ERROR)
              .build()));
          }
        }
      });
      return future;
    }
  }

  @Override
  protected CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request) {
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
  protected CompletableFuture<JoinResponse> join(JoinRequest request) {
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
  protected CompletableFuture<ReconfigureResponse> reconfigure(ReconfigureRequest request) {
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
  protected CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
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
  protected CompletableFuture<InstallResponse> install(InstallRequest request) {
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
