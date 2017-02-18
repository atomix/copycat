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

import io.atomix.copycat.protocol.ProtocolListener;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.server.protocol.request.*;
import io.atomix.copycat.server.protocol.response.*;

/**
 * Raft protocol server connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface RaftProtocolServerConnection extends RaftProtocolConnection, ProtocolServerConnection {

  /**
   * Registers a listener to be called when a join request is received from the client.
   *
   * @param listener The listener to be called when a join request is received from the client.
   * @return The client connection.
   */
  ProtocolServerConnection onJoin(ProtocolListener<JoinRequest, JoinResponse> listener);

  /**
   * Registers a listener to be called when a leave request is received from the client.
   *
   * @param listener The listener to be called when a leave request is received from the client.
   * @return The client connection.
   */
  ProtocolServerConnection onLeave(ProtocolListener<LeaveRequest, LeaveResponse> listener);

  /**
   * Registers a listener to be called when an install request is received from the client.
   *
   * @param listener The listener to be called when an install request is received from the client.
   * @return The client connection.
   */
  ProtocolServerConnection onInstall(ProtocolListener<InstallRequest, InstallResponse> listener);

  /**
   * Registers a listener to be called when a configure request is received from the client.
   *
   * @param listener The listener to be called when a configure request is received from the client.
   * @return The client connection.
   */
  ProtocolServerConnection onConfigure(ProtocolListener<ConfigureRequest, ConfigureResponse> listener);

  /**
   * Registers a listener to be called when a reconfigure request is received from the client.
   *
   * @param listener The listener to be called when a reconfigure request is received from the client.
   * @return The client connection.
   */
  ProtocolServerConnection onReconfigure(ProtocolListener<ReconfigureRequest, ReconfigureResponse> listener);

  /**
   * Registers a listener to be called when an accept request is received from the client.
   *
   * @param listener The listener to be called when an accept request is received from the client.
   * @return The client connection.
   */
  ProtocolServerConnection onAccept(ProtocolListener<AcceptRequest, AcceptResponse> listener);

  /**
   * Registers a listener to be called when a poll request is received from the client.
   *
   * @param listener The listener to be called when a poll request is received from the client.
   * @return The client connection.
   */
  ProtocolServerConnection onPoll(ProtocolListener<PollRequest, PollResponse> listener);

  /**
   * Registers a listener to be called when a vote request is received from the client.
   *
   * @param listener The listener to be called when a vote request is received from the client.
   * @return The client connection.
   */
  ProtocolServerConnection onVote(ProtocolListener<VoteRequest, VoteResponse> listener);

  /**
   * Registers a listener to be called when an append request is received from the client.
   *
   * @param listener The listener to be called when an append request is received from the client.
   * @return The client connection.
   */
  ProtocolServerConnection onAppend(ProtocolListener<AppendRequest, AppendResponse> listener);

}
