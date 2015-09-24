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
package io.atomix.catalog.server.state;

import io.atomix.catalog.client.request.CommandRequest;
import io.atomix.catalog.client.request.KeepAliveRequest;
import io.atomix.catalog.client.request.QueryRequest;
import io.atomix.catalog.client.request.RegisterRequest;
import io.atomix.catalog.client.response.CommandResponse;
import io.atomix.catalog.client.response.KeepAliveResponse;
import io.atomix.catalog.client.response.QueryResponse;
import io.atomix.catalog.client.response.RegisterResponse;
import io.atomix.catalog.server.RaftServer;
import io.atomix.catalog.server.request.*;
import io.atomix.catalog.server.response.*;
import io.atomix.catalyst.util.concurrent.Futures;

import java.util.concurrent.CompletableFuture;

/**
 * Inactive state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class InactiveState extends AbstractState {

  public InactiveState(ServerState context) {
    super(context);
  }

  @Override
  public CompletableFuture<AbstractState> open() {
    try {
      context.getLog().close();
    } catch (Exception e) {
    }
    context.getStateMachine().close();
    return CompletableFuture.completedFuture(this);
  }

  @Override
  public RaftServer.State type() {
    return RaftServer.State.INACTIVE;
  }

  @Override
  protected CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  protected CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  protected CompletableFuture<JoinResponse> join(JoinRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  protected CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  protected CompletableFuture<AppendResponse> append(AppendRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  protected CompletableFuture<PollResponse> poll(PollRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  protected CompletableFuture<VoteResponse> vote(VoteRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  protected CompletableFuture<CommandResponse> command(CommandRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  protected CompletableFuture<QueryResponse> query(QueryRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

}
