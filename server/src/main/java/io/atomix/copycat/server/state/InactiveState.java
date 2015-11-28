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

import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.copycat.client.request.*;
import io.atomix.copycat.client.response.*;
import io.atomix.copycat.server.controller.ServerStateController;
import io.atomix.copycat.server.request.*;
import io.atomix.copycat.server.response.*;

import java.util.concurrent.CompletableFuture;

/**
 * Inactive state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class InactiveState extends RaftState {

  public InactiveState(ServerStateController controller) {
    super(controller);
  }

  @Override
  public Type type() {
    return RaftStateType.INACTIVE;
  }

  @Override
  public CompletableFuture<RegisterResponse> register(RegisterRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<ConnectResponse> connect(ConnectRequest request, Connection connection) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<AcceptResponse> accept(AcceptRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<PublishResponse> publish(PublishRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(ConfigureRequest request) {
    return Futures.exceptionalFuture(new IllegalStateException("inactive state"));
  }

  @Override
  public CompletableFuture<JoinResponse> join(JoinRequest request) {
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
