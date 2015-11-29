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
package io.atomix.copycat.server.controller;

import io.atomix.catalyst.transport.Connection;
import io.atomix.copycat.client.request.*;
import io.atomix.copycat.server.request.*;
import io.atomix.copycat.server.state.RaftState;
import io.atomix.copycat.server.state.ServerContext;

/**
 * Raft server state controller.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class RaftStateController extends ServerStateController<RaftState> {

  public RaftStateController(ServerContext context) {
    super(context);
  }

  @Override
  public void connectClient(Connection connection) {
    connection.handler(RegisterRequest.class, request -> state.register(request));
    connection.handler(ConnectRequest.class, request -> state.connect(request, connection));
    connection.handler(KeepAliveRequest.class, request -> state.keepAlive(request));
    connection.handler(UnregisterRequest.class, request -> state.unregister(request));
    connection.handler(CommandRequest.class, request -> state.command(request));
    connection.handler(QueryRequest.class, request -> state.query(request));
  }

  @Override
  public void disconnectClient(Connection connection) {
    connection.handler(RegisterRequest.class, null);
    connection.handler(ConnectRequest.class, null);
    connection.handler(KeepAliveRequest.class, null);
    connection.handler(UnregisterRequest.class, null);
    connection.handler(CommandRequest.class, null);
    connection.handler(QueryRequest.class, null);
  }

  @Override
  public void connectServer(Connection connection) {
    // Handlers for all request types are registered since requests can be proxied between servers.
    // Note we do not use method references here because the "state" variable changes over time.
    // We have to use lambdas to ensure the request handler points to the current state.
    connection.handler(RegisterRequest.class, request -> state.register(request));
    connection.handler(ConnectRequest.class, request -> state.connect(request, connection));
    connection.handler(AcceptRequest.class, request -> state.accept(request));
    connection.handler(KeepAliveRequest.class, request -> state.keepAlive(request));
    connection.handler(UnregisterRequest.class, request -> state.unregister(request));
    connection.handler(PublishRequest.class, request -> state.publish(request));
    connection.handler(ConfigureRequest.class, request -> state.configure(request));
    connection.handler(JoinRequest.class, request -> state.join(request));
    connection.handler(LeaveRequest.class, request -> state.leave(request));
    connection.handler(AppendRequest.class, request -> state.append(request));
    connection.handler(PollRequest.class, request -> state.poll(request));
    connection.handler(VoteRequest.class, request -> state.vote(request));
    connection.handler(CommandRequest.class, request -> state.command(request));
    connection.handler(QueryRequest.class, request -> state.query(request));
  }

  @Override
  public void disconnectServer(Connection connection) {
    connection.handler(RegisterRequest.class, null);
    connection.handler(ConnectRequest.class, null);
    connection.handler(AcceptRequest.class, null);
    connection.handler(KeepAliveRequest.class, null);
    connection.handler(UnregisterRequest.class, null);
    connection.handler(PublishRequest.class, null);
    connection.handler(ConfigureRequest.class, null);
    connection.handler(JoinRequest.class, null);
    connection.handler(LeaveRequest.class, null);
    connection.handler(AppendRequest.class, null);
    connection.handler(PollRequest.class, null);
    connection.handler(VoteRequest.class, null);
    connection.handler(CommandRequest.class, null);
    connection.handler(QueryRequest.class, null);
  }

}
