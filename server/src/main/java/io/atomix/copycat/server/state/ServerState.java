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

import io.atomix.catalyst.util.Managed;
import io.atomix.copycat.client.request.Request;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.controller.ServerStateController;
import io.atomix.copycat.server.request.JoinRequest;
import io.atomix.copycat.server.request.LeaveRequest;
import io.atomix.copycat.server.response.JoinResponse;
import io.atomix.copycat.server.response.LeaveResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * Abstract state.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ServerState implements Managed<ServerState> {

  /**
   * Server state type.
   */
  public interface Type<T extends ServerState> extends CopycatServer.State {

    /**
     * Creates a new instance of the state.
     *
     * @param controller The state controller.
     * @return The server state.
     */
    T createState(ServerStateController controller);

  }

  protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
  protected final ServerStateController controller;
  private boolean open = true;

  protected ServerState(ServerStateController controller) {
    this.controller = controller;
  }

  /**
   * Returns the Copycat state represented by this state.
   *
   * @return The Copycat state represented by this state.
   */
  public abstract Type type();

  /**
   * Logs a request.
   */
  protected final <R extends Request> R logRequest(R request) {
    LOGGER.debug("{} - Received {}", controller.context().getCluster().getMember().serverAddress(), request);
    return request;
  }

  /**
   * Logs a response.
   */
  protected final <R extends Response> R logResponse(R response) {
    LOGGER.debug("{} - Sent {}", controller.context().getCluster().getMember().serverAddress(), response);
    return response;
  }

  @Override
  public CompletableFuture<ServerState> open() {
    controller.context().checkThread();
    open = true;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isOpen() {
    return open;
  }

  /**
   * Handles a join request.
   */
  public abstract CompletableFuture<JoinResponse> join(JoinRequest request);

  /**
   * Handles a leave request.
   */
  public abstract CompletableFuture<LeaveResponse> leave(LeaveRequest request);

  @Override
  public CompletableFuture<Void> close() {
    controller.context().checkThread();
    open = false;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public boolean isClosed() {
    return !open;
  }

  @Override
  public String toString() {
    return String.format("%s[context=%s]", getClass().getSimpleName(), controller.context());
  }

}
