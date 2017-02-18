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

import io.atomix.copycat.protocol.request.ProtocolRequest;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.serializers.ProtocolRequestSerializer;
import io.atomix.copycat.protocol.serializers.ProtocolResponseSerializer;
import io.atomix.copycat.protocol.tcp.NettyTcpClientConnection;
import io.atomix.copycat.protocol.tcp.TcpOptions;
import io.atomix.copycat.server.protocol.RaftProtocolClientConnection;
import io.atomix.copycat.server.protocol.request.*;
import io.atomix.copycat.server.protocol.response.*;
import io.atomix.copycat.server.protocol.serializers.RaftProtocolRequestSerializer;
import io.atomix.copycat.server.protocol.serializers.RaftProtocolResponseSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;

import java.util.concurrent.CompletableFuture;

/**
 * Raft Netty TCP client connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NettyTcpRaftClientConnection extends NettyTcpClientConnection implements RaftProtocolClientConnection {

  public NettyTcpRaftClientConnection(Channel channel, TcpOptions options) {
    super(channel, options);
  }

  @Override
  @SuppressWarnings("unchecked")
  protected void onMessage(ByteBuf buffer) {
    final long id = buffer.readLong();
    final byte typeId = buffer.readByte();
    if (ProtocolRequest.Type.isProtocolRequest(typeId)) {
      ProtocolRequest.Type type = ProtocolRequest.Type.forId(typeId);
      ProtocolRequestSerializer<?> serializer = RaftProtocolRequestSerializer.forType(type);
      ProtocolRequest request = serializer.readObject(INPUT.get().setByteBuf(buffer), type.type());
      onRequest(id, request);
    } else if (ProtocolResponse.Type.isProtocolResponse(typeId)) {
      ProtocolResponse.Type type = ProtocolResponse.Type.forId(typeId);
      ProtocolResponseSerializer<?> serializer = RaftProtocolResponseSerializer.forType(type);
      ProtocolResponse response = serializer.readObject(INPUT.get().setByteBuf(buffer), type.type());
      onResponse(id, response);
    }
  }

  @Override
  public CompletableFuture<JoinResponse> join(JoinRequest request) {
    return sendRequest(request);
  }

  @Override
  public CompletableFuture<LeaveResponse> leave(LeaveRequest request) {
    return sendRequest(request);
  }

  @Override
  public CompletableFuture<InstallResponse> install(InstallRequest request) {
    return sendRequest(request);
  }

  @Override
  public CompletableFuture<ConfigureResponse> configure(ConfigureRequest request) {
    return sendRequest(request);
  }

  @Override
  public CompletableFuture<ReconfigureResponse> reconfigure(ReconfigureRequest request) {
    return sendRequest(request);
  }

  @Override
  public CompletableFuture<AcceptResponse> accept(AcceptRequest request) {
    return sendRequest(request);
  }

  @Override
  public CompletableFuture<PollResponse> poll(PollRequest request) {
    return sendRequest(request);
  }

  @Override
  public CompletableFuture<VoteResponse> vote(VoteRequest request) {
    return sendRequest(request);
  }

  @Override
  public CompletableFuture<AppendResponse> append(AppendRequest request) {
    return sendRequest(request);
  }
}
