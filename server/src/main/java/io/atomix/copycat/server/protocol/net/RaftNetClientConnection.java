/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.copycat.server.protocol.net;

import io.atomix.copycat.protocol.ProtocolRequestFactory;
import io.atomix.copycat.protocol.net.NetClientConnection;
import io.atomix.copycat.server.protocol.RaftProtocolClientConnection;
import io.atomix.copycat.server.protocol.net.request.*;
import io.atomix.copycat.server.protocol.request.*;
import io.atomix.copycat.server.protocol.response.*;
import io.vertx.core.net.NetSocket;

import java.util.concurrent.CompletableFuture;

/**
 * Raft TCP protocol client connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class RaftNetClientConnection extends NetClientConnection implements RaftProtocolClientConnection {
  public RaftNetClientConnection(NetSocket socket) {
    super(socket);
  }

  @Override
  public CompletableFuture<JoinResponse> join(ProtocolRequestFactory<JoinRequest.Builder, JoinRequest> factory) {
    return sendRequest((NetJoinRequest) factory.build(new NetJoinRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(ProtocolRequestFactory<LeaveRequest.Builder, LeaveRequest> factory) {
    return sendRequest((NetLeaveRequest) factory.build(new NetLeaveRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<InstallResponse> install(ProtocolRequestFactory<InstallRequest.Builder, InstallRequest> factory) {
    return sendRequest((NetInstallRequest) factory.build(new NetInstallRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(ProtocolRequestFactory<ConfigureRequest.Builder, ConfigureRequest> factory) {
    return sendRequest((NetConfigureRequest) factory.build(new NetConfigureRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(ProtocolRequestFactory<ReconfigureRequest.Builder, ReconfigureRequest> factory) {
    return sendRequest((NetReconfigureRequest) factory.build(new NetReconfigureRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<AcceptResponse> accept(ProtocolRequestFactory<AcceptRequest.Builder, AcceptRequest> factory) {
    return sendRequest((NetAcceptRequest) factory.build(new NetAcceptRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<PollResponse> poll(ProtocolRequestFactory<PollRequest.Builder, PollRequest> factory) {
    return sendRequest((NetPollRequest) factory.build(new NetPollRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<VoteResponse> vote(ProtocolRequestFactory<VoteRequest.Builder, VoteRequest> factory) {
    return sendRequest((NetVoteRequest) factory.build(new NetVoteRequest.Builder(id.incrementAndGet())));
  }

  @Override
  public CompletableFuture<AppendResponse> append(ProtocolRequestFactory<AppendRequest.Builder, AppendRequest> factory) {
    return sendRequest((NetAppendRequest) factory.build(new NetAppendRequest.Builder(id.incrementAndGet())));
  }
}
