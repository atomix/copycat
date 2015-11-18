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
import io.atomix.copycat.server.request.*;
import io.atomix.copycat.server.response.*;
import io.atomix.copycat.server.storage.entry.ConfigurationEntry;
import io.atomix.copycat.server.storage.entry.Entry;

import java.util.concurrent.CompletableFuture;

/**
 * Reserve state.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
class ReserveState extends AbstractState {

  public ReserveState(ServerState context) {
    super(context);
  }

  @Override
  public CopycatServer.State type() {
    return CopycatServer.State.RESERVE;
  }

  /**
   * Forwards the given request to the leader if possible.
   */
  protected <T extends Request<T>, U extends Response<U>> CompletableFuture<U> forward(T request) {
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

    // If the request indicates a term that is greater than the current term then
    // assign that term and leader to the current context and step down as leader.
    if (request.term() > context.getTerm() || (request.term() == context.getTerm() && context.getLeader() == null)) {
      context.setTerm(request.term());
      context.setLeader(request.leader());
    }

    return CompletableFuture.completedFuture(logResponse(handleAppend(logRequest(request))));
  }

  /**
   * Handles an append request.
   */
  private AppendResponse handleAppend(AppendRequest request) {
    // If the previous log index is greater than the current configuration version, fail the append.
    if (request.logIndex() > context.getVersion()) {
      return AppendResponse.builder()
        .withStatus(Response.Status.OK)
        .withTerm(context.getTerm())
        .withSucceeded(false)
        .withLogIndex(context.getVersion())
        .build();
    }

    // Iterate through configuration entries. For each configuration entry, update the local configuration.
    for (Entry entry : request.entries()) {
      if (entry instanceof ConfigurationEntry) {
        ConfigurationEntry configuration = (ConfigurationEntry) entry;
        context.configure(configuration.getIndex(), configuration.getActiveMembers(), configuration.getPassiveMembers(), configuration.getReserveMembers());
      }
    }

    // Response successfully with the latest configuration index.
    return AppendResponse.builder()
      .withStatus(Response.Status.OK)
      .withTerm(context.getTerm())
      .withSucceeded(true)
      .withLogIndex(context.getVersion())
      .build();
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
  protected CompletableFuture<HeartbeatResponse> heartbeat(HeartbeatRequest request) {
    context.checkThread();
    logRequest(request);

    if (context.getLeader() == null) {
      return CompletableFuture.completedFuture(logResponse(HeartbeatResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(RaftError.Type.NO_LEADER_ERROR)
        .build()));
    } else {
      return this.<HeartbeatRequest, HeartbeatResponse>forward(request).thenApply(this::logResponse);
    }
  }

}
