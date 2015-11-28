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
import io.atomix.copycat.client.request.*;
import io.atomix.copycat.client.response.*;
import io.atomix.copycat.server.controller.ServerStateController;
import io.atomix.copycat.server.request.*;
import io.atomix.copycat.server.response.*;

import java.util.concurrent.CompletableFuture;

/**
 * Raft server state.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class RaftState extends ServerState {

  public RaftState(ServerStateController controller) {
    super(controller);
  }

  /**
   * Handles a register request.
   */
  public abstract CompletableFuture<RegisterResponse> register(RegisterRequest request);

  /**
   * Handles a connect request.
   */
  public abstract CompletableFuture<ConnectResponse> connect(ConnectRequest request, Connection connection);

  /**
   * Handles an accept request.
   */
  public abstract CompletableFuture<AcceptResponse> accept(AcceptRequest request);

  /**
   * Handles a keep alive request.
   */
  public abstract CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request);

  /**
   * Handles an unregister request.
   */
  public abstract CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request);

  /**
   * Handles a publish request.
   */
  public abstract CompletableFuture<PublishResponse> publish(PublishRequest request);

  /**
   * Handles a configure request.
   */
  public abstract CompletableFuture<ConfigureResponse> configure(ConfigureRequest request);

  /**
   * Handles an append request.
   */
  public abstract CompletableFuture<AppendResponse> append(AppendRequest request);

  /**
   * Handles a poll request.
   */
  public abstract CompletableFuture<PollResponse> poll(PollRequest request);

  /**
   * Handles a vote request.
   */
  public abstract CompletableFuture<VoteResponse> vote(VoteRequest request);

  /**
   * Handles a command request.
   */
  public abstract CompletableFuture<CommandResponse> command(CommandRequest request);

  /**
   * Handles a query request.
   */
  public abstract CompletableFuture<QueryResponse> query(QueryRequest request);

}
