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
import io.atomix.copycat.client.error.RaftError;
import io.atomix.copycat.client.request.*;
import io.atomix.copycat.client.response.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.request.*;
import io.atomix.copycat.server.response.*;

import java.util.concurrent.CompletableFuture;

/**
 * The reserve state receives configuration changes from followers and proxies other requests
 * to active members of the cluster.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ReserveState extends AbstractState {

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
        context.resetLog();
      }
    }).thenApply(v -> this);
  }

  /**
   * Forwards the given request to the leader if possible.
   */
  protected <T extends Request, U extends Response> CompletableFuture<U> forward(T request) {
    CompletableFuture<U> future = new CompletableFuture<>();
    context.getConnections().getConnection(context.getLeader().serverAddress()).whenComplete((connection, connectError) -> {
      if (connectError == null) {
        connection.<T, U>send(request).whenComplete((response, responseError) -> {
          if (responseError == null) {
            future.complete(response);
          } else {
            future.completeExceptionally(responseError);
          }
        });
      } else {
        future.completeExceptionally(connectError);
      }
    });
    return future;
  }

  @Override
  protected CompletableFuture<AppendResponse> append(AppendRequest request) {
    context.checkThread();
    logRequest(request);

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm() || (request.term() == context.getTerm() && context.getLeader() == null)) {
      context.setTerm(request.term());
      context.setLeader(request.leader());
    }

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
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<VoteResponse> vote(VoteRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(VoteResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<CommandResponse> command(CommandRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(CommandResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<CommandRequest, CommandResponse>forward(request).thenApply(this::logResponse);
    }
  }

  @Override
  protected CompletableFuture<QueryResponse> query(QueryRequest request) {
    context.checkThread();
    logRequest(request);
    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(QueryResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<QueryRequest, QueryResponse>forward(request).thenApply(this::logResponse);
    }
  }

  @Override
  protected CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(RegisterResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<ConnectResponse> connect(ConnectRequest request, Connection connection) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(ConnectResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<AcceptResponse> accept(AcceptRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(AcceptResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(KeepAliveResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withLeader(context.getLeader() != null ? context.getLeader().serverAddress() : null)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(UnregisterResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<PublishResponse> publish(PublishRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(PublishResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  protected CompletableFuture<JoinResponse> join(JoinRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(JoinResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<JoinRequest, JoinResponse>forward(request).thenApply(this::logResponse);
    }
  }

  @Override
  protected CompletableFuture<ReconfigureResponse> reconfigure(ReconfigureRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(ReconfigureResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<ReconfigureRequest, ReconfigureResponse>forward(request).thenApply(this::logResponse);
    }
  }

  @Override
  protected CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(LeaveResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<LeaveRequest, LeaveResponse>forward(request).thenApply(this::logResponse);
    }
  }

  @Override
  protected CompletableFuture<ConfigureResponse> configure(ConfigureRequest request) {
    context.checkThread();
    logRequest(request);

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm() || (request.term() == context.getTerm() && context.getLeader() == null)) {
      context.setTerm(request.term());
      context.setLeader(request.leader());
    }

    // Store the previous member type for comparison to determine whether this node should transition.
    Member.Type previousType = context.getCluster().member().type();

    // Configure the cluster membership.
    context.getClusterState().configure(request.index(), request.members());

    // If the local member type changed, transition the state as appropriate.
    // ACTIVE servers are initialized to the FOLLOWER state but may transition to CANDIDATE or LEADER.
    // PASSIVE servers are transitioned to the PASSIVE state.
    Member.Type type = context.getCluster().member().type();
    if (previousType != type) {
      switch (type) {
        case ACTIVE:
          context.transition(CopycatServer.State.FOLLOWER);
          break;
        case PROMOTABLE:
        case PASSIVE:
          context.transition(CopycatServer.State.PASSIVE);
          break;
        case RESERVE:
          context.transition(CopycatServer.State.RESERVE);
          break;
        default:
          transition(CopycatServer.State.INACTIVE);
      }
    }

    return CompletableFuture.completedFuture(logResponse(ConfigureResponse.builder()
      .withStatus(Response.Status.OK)
      .build()));
  }

  @Override
  protected CompletableFuture<InstallResponse> install(InstallRequest request) {
    context.checkThread();
    logRequest(request);

    return CompletableFuture.completedFuture(logResponse(InstallResponse.builder()
      .withStatus(Response.Status.ERROR)
      .withError(RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR)
      .build()));
  }

  @Override
  public CompletableFuture<Void> close() {
    return super.close().thenRun(() -> {
      if (type() == CopycatServer.State.RESERVE) {
        context.resetLog();
      }
    });
  }

}
