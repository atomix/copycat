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
package io.atomix.copycat.server.state;

import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Managed;
import io.atomix.copycat.protocol.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.protocol.*;

import java.util.concurrent.CompletableFuture;

/**
 * Server state interface.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ServerState extends Managed<ServerState> {

  /**
   * Returns the server state type.
   *
   * @return The server state type.
   */
  CopycatServer.State type();

  /**
   * Handles a register request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<RegisterResponse> register(RegisterRequest request);

  /**
   * Handles a connect request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<ConnectResponse> connect(ConnectRequest request, Connection connection);

  /**
   * Handles a keep alive request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request);

  /**
   * Handles an unregister request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request);

  /**
   * Handles a reset request.
   *
   * @param request The request to handle
   */
  void reset(ResetRequest request);

  /**
   * Handles a configure request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<ConfigureResponse> configure(ConfigureRequest request);

  /**
   * Handles an install request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<InstallResponse> install(InstallRequest request);

  /**
   * Handles a join request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<JoinResponse> join(JoinRequest request);

  /**
   * Handles a configure request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<ReconfigureResponse> reconfigure(ReconfigureRequest request);

  /**
   * Handles a leave request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<LeaveResponse> leave(LeaveRequest request);

  /**
   * Handles an append request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<AppendResponse> append(AppendRequest request);

  /**
   * Handles a poll request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<PollResponse> poll(PollRequest request);

  /**
   * Handles a vote request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<VoteResponse> vote(VoteRequest request);

  /**
   * Handles a command request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<CommandResponse> command(CommandRequest request);

  /**
   * Handles a query request.
   *
   * @param request The request to handle.
   * @return A completable future to be completed with the request response.
   */
  CompletableFuture<QueryResponse> query(QueryRequest request);

}
