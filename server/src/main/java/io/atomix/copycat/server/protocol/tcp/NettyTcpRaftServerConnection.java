/*
 * Copyright 2017 the original author or authors.
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
package io.atomix.copycat.server.protocol.tcp;

import io.atomix.copycat.protocol.ProtocolListener;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.protocol.request.ProtocolRequest;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.serializers.ProtocolRequestSerializer;
import io.atomix.copycat.protocol.serializers.ProtocolResponseSerializer;
import io.atomix.copycat.protocol.tcp.NettyTcpServerConnection;
import io.atomix.copycat.protocol.tcp.TcpOptions;
import io.atomix.copycat.server.protocol.RaftProtocolServerConnection;
import io.atomix.copycat.server.protocol.request.*;
import io.atomix.copycat.server.protocol.response.*;
import io.atomix.copycat.server.protocol.serializers.RaftProtocolRequestSerializer;
import io.atomix.copycat.server.protocol.serializers.RaftProtocolResponseSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

/**
 * Raft Netty TCP server connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NettyTcpRaftServerConnection extends NettyTcpServerConnection implements RaftProtocolServerConnection {
  private ProtocolListener<JoinRequest, JoinResponse> joinListener;
  private ProtocolListener<LeaveRequest, LeaveResponse> leaveListener;
  private ProtocolListener<InstallRequest, InstallResponse> installListener;
  private ProtocolListener<ConfigureRequest, ConfigureResponse> configureListener;
  private ProtocolListener<ReconfigureRequest, ReconfigureResponse> reconfigureListener;
  private ProtocolListener<AcceptRequest, AcceptResponse> acceptListener;
  private ProtocolListener<PollRequest, PollResponse> pollListener;
  private ProtocolListener<VoteRequest, VoteResponse> voteListener;
  private ProtocolListener<AppendRequest, AppendResponse> appendListener;

  public NettyTcpRaftServerConnection(Channel channel, TcpOptions options) {
    super(channel, options);
  }

  @Override
  protected void onMessage(ByteBuf buffer) {
    final long id = buffer.readLong();
    final byte typeId = buffer.readByte();
    if (RaftProtocolRequest.Type.isProtocolRequest(typeId)) {
      onRequest(id, readRequest(typeId, buffer));
    } else if (RaftProtocolResponse.Type.isProtocolResponse(typeId)) {
      onResponse(id, readResponse(typeId, buffer));
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void writeRequest(ProtocolRequest request, ByteBuf buffer) {
    ProtocolRequestSerializer serializer = RaftProtocolRequestSerializer.forType(request.type());
    serializer.writeObject(OUTPUT.get().setByteBuf(buffer), request);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected ProtocolRequest readRequest(int typeId, ByteBuf buffer) {
    ProtocolRequest.Type type = RaftProtocolRequest.Type.forId(typeId);
    ProtocolRequestSerializer<?> serializer = RaftProtocolRequestSerializer.forType(type);
    return serializer.readObject(INPUT.get().setByteBuf(buffer), type.type());
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void writeResponse(ProtocolResponse response, ByteBuf buffer) {
    ProtocolResponseSerializer serializer = RaftProtocolResponseSerializer.forType(response.type());
    serializer.writeObject(OUTPUT.get().setByteBuf(buffer), response);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected ProtocolResponse readResponse(int typeId, ByteBuf buffer) {
    ProtocolResponse.Type type = RaftProtocolResponse.Type.forId(typeId);
    ProtocolResponseSerializer<?> serializer = RaftProtocolResponseSerializer.forType(type);
    return serializer.readObject(INPUT.get().setByteBuf(buffer), type.type());
  }

  @Override
  protected boolean onRequest(long id, ProtocolRequest request) {
    if (super.onRequest(id, request)) {
      return true;
    }

    if (request.type() == RaftProtocolRequest.Type.JOIN) {
      joinListener.onRequest((JoinRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else if (request.type() == RaftProtocolRequest.Type.LEAVE) {
      leaveListener.onRequest((LeaveRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else if (request.type() == RaftProtocolRequest.Type.INSTALL) {
      installListener.onRequest((InstallRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else if (request.type() == RaftProtocolRequest.Type.CONFIGURE) {
      configureListener.onRequest((ConfigureRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else if (request.type() == RaftProtocolRequest.Type.RECONFIGURE) {
      reconfigureListener.onRequest((ReconfigureRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else if (request.type() == RaftProtocolRequest.Type.ACCEPT) {
      acceptListener.onRequest((AcceptRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else if (request.type() == RaftProtocolRequest.Type.POLL) {
      pollListener.onRequest((PollRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else if (request.type() == RaftProtocolRequest.Type.VOTE) {
      voteListener.onRequest((VoteRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else if (request.type() == RaftProtocolRequest.Type.APPEND) {
      appendListener.onRequest((AppendRequest) request)
        .whenComplete((response, error) -> {
          if (error == null) {
            sendResponse(id, response);
          }
        });
    } else {
      return false;
    }
    return true;
  }

  @Override
  public ProtocolServerConnection onJoin(ProtocolListener<JoinRequest, JoinResponse> listener) {
    this.joinListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onLeave(ProtocolListener<LeaveRequest, LeaveResponse> listener) {
    this.leaveListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onInstall(ProtocolListener<InstallRequest, InstallResponse> listener) {
    this.installListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onConfigure(ProtocolListener<ConfigureRequest, ConfigureResponse> listener) {
    this.configureListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onReconfigure(ProtocolListener<ReconfigureRequest, ReconfigureResponse> listener) {
    this.reconfigureListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onAccept(ProtocolListener<AcceptRequest, AcceptResponse> listener) {
    this.acceptListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onPoll(ProtocolListener<PollRequest, PollResponse> listener) {
    this.pollListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onVote(ProtocolListener<VoteRequest, VoteResponse> listener) {
    this.voteListener = listener;
    return this;
  }

  @Override
  public ProtocolServerConnection onAppend(ProtocolListener<AppendRequest, AppendResponse> listener) {
    this.appendListener = listener;
    return this;
  }
}
