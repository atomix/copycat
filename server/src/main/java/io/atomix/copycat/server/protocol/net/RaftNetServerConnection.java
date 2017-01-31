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

import io.atomix.copycat.protocol.ProtocolListener;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.protocol.net.NetServerConnection;
import io.atomix.copycat.protocol.net.request.NetRequest;
import io.atomix.copycat.protocol.net.response.NetResponse;
import io.atomix.copycat.server.protocol.RaftProtocolServerConnection;
import io.atomix.copycat.server.protocol.net.request.RaftNetRequest;
import io.atomix.copycat.server.protocol.net.response.*;
import io.atomix.copycat.server.protocol.request.*;
import io.atomix.copycat.server.protocol.response.*;
import io.atomix.copycat.util.buffer.HeapBuffer;
import io.vertx.core.net.NetSocket;

/**
 * Raft TCP protocol server connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class RaftNetServerConnection extends NetServerConnection implements RaftProtocolServerConnection {
  private ProtocolListener<JoinRequest, JoinResponse.Builder, JoinResponse> joinListener;
  private ProtocolListener<LeaveRequest, LeaveResponse.Builder, LeaveResponse> leaveListener;
  private ProtocolListener<InstallRequest, InstallResponse.Builder, InstallResponse> installListener;
  private ProtocolListener<ConfigureRequest, ConfigureResponse.Builder, ConfigureResponse> configureListener;
  private ProtocolListener<ReconfigureRequest, ReconfigureResponse.Builder, ReconfigureResponse> reconfigureListener;
  private ProtocolListener<AcceptRequest, AcceptResponse.Builder, AcceptResponse> acceptListener;
  private ProtocolListener<PollRequest, PollResponse.Builder, PollResponse> pollListener;
  private ProtocolListener<VoteRequest, VoteResponse.Builder, VoteResponse> voteListener;
  private ProtocolListener<AppendRequest, AppendResponse.Builder, AppendResponse> appendListener;

  public RaftNetServerConnection(NetSocket socket) {
    super(socket);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void handleMessage(int id, byte[] bytes) {
    if (RaftNetRequest.Type.isProtocolRequest(id)) {
      NetRequest.Type<?> type = RaftNetRequest.Type.forId(id);
      NetRequest request = type.serializer().readObject(HeapBuffer.wrap(bytes), type.type());
      onRequest(request);
    } else if (RaftNetResponse.Type.isProtocolResponse(id)) {
      NetResponse.Type<?> type = RaftNetResponse.Type.forId(id);
      NetResponse response = type.serializer().readObject(HeapBuffer.wrap(bytes), type.type());
      onResponse(response);
    }
  }

  @Override
  protected boolean onRequest(NetRequest request) {
    if (super.onRequest(request)) {
      return true;
    }

    if (request.type() == RaftNetRequest.Type.JOIN) {
      joinListener.onRequest((JoinRequest) request, new NetJoinResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetJoinResponse) response);
          }
        });
    } else if (request.type() == RaftNetRequest.Type.LEAVE) {
      leaveListener.onRequest((LeaveRequest) request, new NetLeaveResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetLeaveResponse) response);
          }
        });
    } else if (request.type() == RaftNetRequest.Type.INSTALL) {
      installListener.onRequest((InstallRequest) request, new NetInstallResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetInstallResponse) response);
          }
        });
    } else if (request.type() == RaftNetRequest.Type.CONFIGURE) {
      configureListener.onRequest((ConfigureRequest) request, new NetConfigureResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetConfigureResponse) response);
          }
        });
    } else if (request.type() == RaftNetRequest.Type.RECONFIGURE) {
      reconfigureListener.onRequest((ReconfigureRequest) request, new NetReconfigureResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetReconfigureResponse) response);
          }
        });
    } else if (request.type() == RaftNetRequest.Type.ACCEPT) {
      acceptListener.onRequest((AcceptRequest) request, new NetAcceptResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetAcceptResponse) response);
          }
        });
    } else if (request.type() == RaftNetRequest.Type.POLL) {
      pollListener.onRequest((PollRequest) request, new NetPollResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetPollResponse) response);
          }
        });
    } else if (request.type() == RaftNetRequest.Type.VOTE) {
      voteListener.onRequest((VoteRequest) request, new NetVoteResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetVoteResponse) response);
          }
        });
    } else if (request.type() == RaftNetRequest.Type.APPEND) {
      appendListener.onRequest((AppendRequest) request, new NetAppendResponse.Builder(request.id()))
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse((NetAppendResponse) response);
          }
        });
    } else {
      return false;
    }
    return true;
  }

  @Override
  public ProtocolServerConnection onJoin(ProtocolListener<JoinRequest, JoinResponse.Builder, JoinResponse> listener) {
    this.joinListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onLeave(ProtocolListener<LeaveRequest, LeaveResponse.Builder, LeaveResponse> listener) {
    this.leaveListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onInstall(ProtocolListener<InstallRequest, InstallResponse.Builder, InstallResponse> listener) {
    this.installListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onConfigure(ProtocolListener<ConfigureRequest, ConfigureResponse.Builder, ConfigureResponse> listener) {
    this.configureListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onReconfigure(ProtocolListener<ReconfigureRequest, ReconfigureResponse.Builder, ReconfigureResponse> listener) {
    this.reconfigureListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onAccept(ProtocolListener<AcceptRequest, AcceptResponse.Builder, AcceptResponse> listener) {
    this.acceptListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onPoll(ProtocolListener<PollRequest, PollResponse.Builder, PollResponse> listener) {
    this.pollListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onVote(ProtocolListener<VoteRequest, VoteResponse.Builder, VoteResponse> listener) {
    this.voteListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onAppend(ProtocolListener<AppendRequest, AppendResponse.Builder, AppendResponse> listener) {
    this.appendListener = listener;
    return this;
  }
}
