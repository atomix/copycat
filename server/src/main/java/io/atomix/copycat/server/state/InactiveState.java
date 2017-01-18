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

import io.atomix.catalyst.concurrent.Futures;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.protocol.request.*;
import io.atomix.copycat.protocol.response.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.protocol.request.*;
import io.atomix.copycat.server.protocol.response.*;
import io.atomix.copycat.server.storage.system.Configuration;

import java.util.concurrent.CompletableFuture;

/**
 * Inactive state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class InactiveState extends AbstractState {

  public InactiveState(ServerContext context) {
    super(context);
  }

  @Override
  public CopycatServer.State type() {
    return CopycatServer.State.INACTIVE;
  }

  @Override
  public CompletableFuture<ConfigureResponse> onConfigure(ConfigureRequest request, ConfigureResponse.Builder responseBuilder) {
    context.checkThread();
    logRequest(request);
    updateTermAndLeader(request.term(), request.leader());

    Configuration configuration = new Configuration(request.index(), request.term(), request.timestamp(), request.members());

    // Configure the cluster membership. This will cause this server to transition to the
    // appropriate state if its type has changed.
    context.getClusterState().configure(configuration);

    // If the configuration is already committed, commit it to disk.
    // Check against the actual cluster Configuration rather than the received configuration in
    // case the received configuration was an older configuration that was not applied.
    if (context.getCommitIndex() >= context.getClusterState().getConfiguration().index()) {
      context.getClusterState().commit();
    }

    return CompletableFuture.completedFuture(logResponse(
      responseBuilder
        .withStatus(ProtocolResponse.Status.OK)
        .build()));
  }

  @Override
  public CompletableFuture<RegisterResponse> onRegister(RegisterRequest request, RegisterResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<ConnectResponse> onConnect(ConnectRequest request, ConnectResponse.Builder responseBuilder, ProtocolServerConnection connection) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<AcceptResponse> onAccept(AcceptRequest request, AcceptResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> onKeepAlive(KeepAliveRequest request, KeepAliveResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<UnregisterResponse> onUnregister(UnregisterRequest request, UnregisterResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<PublishResponse> onPublish(PublishRequest request, PublishResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<InstallResponse> onInstall(InstallRequest request, InstallResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<JoinResponse> onJoin(JoinRequest request, JoinResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<ReconfigureResponse> onReconfigure(ReconfigureRequest request, ReconfigureResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<LeaveResponse> onLeave(LeaveRequest request, LeaveResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<AppendResponse> onAppend(AppendRequest request, AppendResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<PollResponse> onPoll(PollRequest request, PollResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<VoteResponse> onVote(VoteRequest request, VoteResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<CommandResponse> onCommand(CommandRequest request, CommandResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<QueryResponse> onQuery(QueryRequest request, QueryResponse.Builder responseBuilder) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }
}
