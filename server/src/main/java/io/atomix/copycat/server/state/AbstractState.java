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
 * limitations under the License.
 */
package io.atomix.copycat.server.state;

import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Managed;
import io.atomix.copycat.protocol.*;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.protocol.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
abstract class AbstractState implements Managed<AbstractState> {
  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected final ServerContext context;
  private boolean open = true;

  protected AbstractState(ServerContext context) {
    this.context = context;
  }

  /**
   * Returns the Copycat state represented by this state.
   *
   * @return The Copycat state represented by this state.
   */
  public abstract CopycatServer.State type();

  /**
   * Logs a request.
   */
  protected final <R extends Request> R logRequest(R request) {
    LOGGER.debug("{} - Received {}", context.getCluster().member().address(), request);
    return request;
  }

  /**
   * Logs a response.
   */
  protected final <R extends Response> R logResponse(R response) {
    LOGGER.debug("{} - Sent {}", context.getCluster().member().address(), response);
    return response;
  }

  @Override
  public CompletableFuture<AbstractState> open() {
    context.checkThread();
    open = true;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  /**
   * Forwards the given request to the leader if possible.
   */
  protected <T extends Request, U extends Response> CompletableFuture<U> forward(T request) {
    CompletableFuture<U> future = new CompletableFuture<>();
    context.getConnections().getConnection(context.getLeader().serverAddress()).whenComplete((connection, connectError) -> {
      if (connectError == null) {
        connection.<T, U>send(request).whenComplete((response, responseError) -> {
          if (responseError == null) {
            future.complete(response);
          } else {
            future.completeExceptionally(responseError);
          }
        });
      } else {
        future.completeExceptionally(connectError);
      }
    });
    return future;
  }

  /**
   * Updates the term and leader.
   */
  protected boolean updateTermAndLeader(long term, int leader) {
    // If the request indicates a term that is greater than the current term or no leader has been
    // set for the current term, update leader and term.
    if (term > context.getTerm() || (term == context.getTerm() && context.getLeader() == null && leader != 0)) {
      context.setTerm(term);
      context.setLeader(leader);

      // Reset the current cluster configuration to the last committed configuration when a leader change occurs.
      context.getClusterState().reset();
      return true;
    }
    return false;
  }

  /**
   * Handles a register request.
   */
  protected abstract CompletableFuture<RegisterResponse> register(RegisterRequest request);

  /**
   * Handles a connect request.
   */
  protected abstract CompletableFuture<ConnectResponse> connect(ConnectRequest request, Connection connection);

  /**
   * Handles an accept request.
   */
  protected abstract CompletableFuture<AcceptResponse> accept(AcceptRequest request);

  /**
   * Handles a keep alive request.
   */
  protected abstract CompletableFuture<KeepAliveResponse> keepAlive(KeepAliveRequest request);

  /**
   * Handles an unregister request.
   */
  protected abstract CompletableFuture<UnregisterResponse> unregister(UnregisterRequest request);

  /**
   * Handles a publish request.
   */
  protected abstract CompletableFuture<PublishResponse> publish(PublishRequest request);

  /**
   * Handles a configure request.
   */
  protected abstract CompletableFuture<ConfigureResponse> configure(ConfigureRequest request);

  /**
   * Handles an install request.
   */
  protected abstract CompletableFuture<InstallResponse> install(InstallRequest request);

  /**
   * Handles a join request.
   */
  protected abstract CompletableFuture<JoinResponse> join(JoinRequest request);

  /**
   * Handles a configure request.
   */
  protected abstract CompletableFuture<ReconfigureResponse> reconfigure(ReconfigureRequest request);

  /**
   * Handles a leave request.
   */
  protected abstract CompletableFuture<LeaveResponse> leave(LeaveRequest request);

  /**
   * Handles an append request.
   */
  protected abstract CompletableFuture<AppendResponse> append(AppendRequest request);

  /**
   * Handles a poll request.
   */
  protected abstract CompletableFuture<PollResponse> poll(PollRequest request);

  /**
   * Handles a vote request.
   */
  protected abstract CompletableFuture<VoteResponse> vote(VoteRequest request);

  /**
   * Handles a command request.
   */
  protected abstract CompletableFuture<CommandResponse> command(CommandRequest request);

  /**
   * Handles a query request.
   */
  protected abstract CompletableFuture<QueryResponse> query(QueryRequest request);

  @Override
  public CompletableFuture<Void> close() {
    context.checkThread();
    open = false;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  @Override
  public String toString() {
    return String.format("%s[context=%s]", getClass().getSimpleName(), context);
  }

}
