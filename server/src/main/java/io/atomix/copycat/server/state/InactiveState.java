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
import io.atomix.catalyst.transport.Connection;
import io.atomix.copycat.protocol.CloseSessionRequest;
import io.atomix.copycat.protocol.CloseSessionResponse;
import io.atomix.copycat.protocol.CommandRequest;
import io.atomix.copycat.protocol.CommandResponse;
import io.atomix.copycat.protocol.ConnectRequest;
import io.atomix.copycat.protocol.ConnectResponse;
import io.atomix.copycat.protocol.KeepAliveRequest;
import io.atomix.copycat.protocol.KeepAliveResponse;
import io.atomix.copycat.protocol.MetadataRequest;
import io.atomix.copycat.protocol.MetadataResponse;
import io.atomix.copycat.protocol.OpenSessionRequest;
import io.atomix.copycat.protocol.OpenSessionResponse;
import io.atomix.copycat.protocol.QueryRequest;
import io.atomix.copycat.protocol.QueryResponse;
import io.atomix.copycat.protocol.ResetRequest;
import io.atomix.copycat.protocol.Response;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.protocol.AppendRequest;
import io.atomix.copycat.server.protocol.AppendResponse;
import io.atomix.copycat.server.protocol.ConfigureRequest;
import io.atomix.copycat.server.protocol.ConfigureResponse;
import io.atomix.copycat.server.protocol.InstallRequest;
import io.atomix.copycat.server.protocol.InstallResponse;
import io.atomix.copycat.server.protocol.JoinRequest;
import io.atomix.copycat.server.protocol.JoinResponse;
import io.atomix.copycat.server.protocol.LeaveRequest;
import io.atomix.copycat.server.protocol.LeaveResponse;
import io.atomix.copycat.server.protocol.PollRequest;
import io.atomix.copycat.server.protocol.PollResponse;
import io.atomix.copycat.server.protocol.ReconfigureRequest;
import io.atomix.copycat.server.protocol.ReconfigureResponse;
import io.atomix.copycat.server.protocol.VoteRequest;
import io.atomix.copycat.server.protocol.VoteResponse;
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
  public CompletableFuture<ConfigureResponse> configure(ConfigureRequest request) {
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

    return CompletableFuture.completedFuture(logResponse(ConfigureResponse.builder()
      .withStatus(Response.Status.OK)
      .build()));
  }

  @Override
  public CompletableFuture<MetadataResponse> metadata(MetadataRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<ConnectResponse> connect(ConnectRequest request, Connection connection) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<OpenSessionResponse> openSession(OpenSessionRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<CloseSessionResponse> closeSession(CloseSessionRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public void reset(ResetRequest request) {
  }

  @Override
  public CompletableFuture<InstallResponse> install(InstallRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<JoinResponse> join(JoinRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(ReconfigureRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<VoteResponse> vote(VoteRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<CommandResponse> command(CommandRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<QueryResponse> query(QueryRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

}
