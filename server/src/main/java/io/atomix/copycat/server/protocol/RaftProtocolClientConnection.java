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
import io.atomix.copycat.protocol.ProtocolRequestFactory;
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
   * @param factory The join request factory.
   * @return A completable future to be completed once the join response is received.
   */
  CompletableFuture<JoinResponse> join(ProtocolRequestFactory<JoinRequest.Builder, JoinRequest> factory);

  /**
   * Sends a leave request to the server.
   *
   * @param factory The leave request factory.
   * @return A completable future to be completed once the leave response is received.
   */
  CompletableFuture<LeaveResponse> leave(ProtocolRequestFactory<LeaveRequest.Builder, LeaveRequest> factory);

  /**
   * Sends an install request to the server.
   *
   * @param factory The install request factory.
   * @return A completable future to be completed once the install response is received.
   */
  CompletableFuture<InstallResponse> install(ProtocolRequestFactory<InstallRequest.Builder, InstallRequest> factory);

  /**
   * Sends a configure request to the server.
   *
   * @param factory The configure request factory.
   * @return A completable future to be completed once the configure response is received.
   */
  CompletableFuture<ConfigureResponse> configure(ProtocolRequestFactory<ConfigureRequest.Builder, ConfigureRequest> factory);

  /**
   * Sends a reconfigure request to the server.
   *
   * @param factory The reconfigure request factory.
   * @return A completable future to be completed once the reconfigure response is received.
   */
  CompletableFuture<ReconfigureResponse> reconfigure(ProtocolRequestFactory<ReconfigureRequest.Builder, ReconfigureRequest> factory);

  /**
   * Sends an accept request to the server.
   *
   * @param factory The accept request factory.
   * @return A completable future to be completed once the accept response is received.
   */
  CompletableFuture<AcceptResponse> accept(ProtocolRequestFactory<AcceptRequest.Builder, AcceptRequest> factory);

  /**
   * Sends a poll request to the server.
   *
   * @param factory The poll request factory.
   * @return A completable future to be completed once the poll response is received.
   */
  CompletableFuture<PollResponse> poll(ProtocolRequestFactory<PollRequest.Builder, PollRequest> factory);

  /**
   * Sends a vote request to the server.
   *
   * @param factory The vote request factory.
   * @return A completable future to be completed once the vote response is received.
   */
  CompletableFuture<VoteResponse> vote(ProtocolRequestFactory<VoteRequest.Builder, VoteRequest> factory);

  /**
   * Sends an append request to the server.
   *
   * @param factory The append request factory.
   * @return A completable future to be completed once the append response is received.
   */
  CompletableFuture<AppendResponse> append(ProtocolRequestFactory<AppendRequest.Builder, AppendRequest> factory);

}
