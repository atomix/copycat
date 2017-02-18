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
package io.atomix.copycat.server.protocol;

import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.atomix.copycat.server.protocol.request.*;
import io.atomix.copycat.server.protocol.response.*;

import java.util.concurrent.CompletableFuture;

/**
 * Raft protocol client conneciton.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface RaftProtocolClientConnection extends RaftProtocolConnection, ProtocolClientConnection {

  /**
   * Sends a join request to the server.
   *
   * @param request The join request.
   * @return A completable future to be completed once the join response is received.
   */
  CompletableFuture<JoinResponse> join(JoinRequest request);

  /**
   * Sends a leave request to the server.
   *
   * @param request The leave request.
   * @return A completable future to be completed once the leave response is received.
   */
  CompletableFuture<LeaveResponse> leave(LeaveRequest request);

  /**
   * Sends an install request to the server.
   *
   * @param request The install request.
   * @return A completable future to be completed once the install response is received.
   */
  CompletableFuture<InstallResponse> install(InstallRequest request);

  /**
   * Sends a configure request to the server.
   *
   * @param request The configure request.
   * @return A completable future to be completed once the configure response is received.
   */
  CompletableFuture<ConfigureResponse> configure(ConfigureRequest request);

  /**
   * Sends a reconfigure request to the server.
   *
   * @param request The reconfigure request.
   * @return A completable future to be completed once the reconfigure response is received.
   */
  CompletableFuture<ReconfigureResponse> reconfigure(ReconfigureRequest request);

  /**
   * Sends an accept request to the server.
   *
   * @param request The accept request.
   * @return A completable future to be completed once the accept response is received.
   */
  CompletableFuture<AcceptResponse> accept(AcceptRequest request);

  /**
   * Sends a poll request to the server.
   *
   * @param request The poll request.
   * @return A completable future to be completed once the poll response is received.
   */
  CompletableFuture<PollResponse> poll(PollRequest request);

  /**
   * Sends a vote request to the server.
   *
   * @param request The vote request.
   * @return A completable future to be completed once the vote response is received.
   */
  CompletableFuture<VoteResponse> vote(VoteRequest request);

  /**
   * Sends an append request to the server.
   *
   * @param request The append request.
   * @return A completable future to be completed once the append response is received.
   */
  CompletableFuture<AppendResponse> append(AppendRequest request);

}
