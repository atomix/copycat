/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.copycat.client.session;

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Builder;
import io.atomix.copycat.Command;
import io.atomix.copycat.Operation;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.CommunicationStrategies;
import io.atomix.copycat.client.CommunicationStrategy;
import io.atomix.copycat.client.CopycatClient;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Copycat client proxy.
 */
public interface CopycatSession {

  /**
   * Returns the unique session identifier.
   *
   * @return The session identifier.
   */
  long id();

  /**
   * Returns the client proxy name.
   *
   * @return The client proxy name.
   */
  String name();

  /**
   * Returns the client proxy type.
   *
   * @return The client proxy type.
   */
  String type();

  /**
   * Submits an operation to the Copycat cluster.
   * <p>
   * This method is provided for convenience. The submitted {@link Operation} must be an instance
   * of {@link Command} or {@link Query}.
   *
   * @param operation The operation to submit.
   * @param <T> The operation result type.
   * @return A completable future to be completed with the operation result.
   * @throws IllegalArgumentException If the {@link Operation} is not an instance of {@link Command} or {@link Query}.
   * @throws NullPointerException if {@code operation} is null
   */
  default <T> CompletableFuture<T> submit(Operation<T> operation) {
    Assert.notNull(operation, "operation");
    if (operation instanceof Command) {
      return submit((Command<T>) operation);
    } else if (operation instanceof Query) {
      return submit((Query<T>) operation);
    } else {
      throw new IllegalArgumentException("unknown operation type");
    }
  }

  /**
   * Submits a command to the Copycat cluster.
   * <p>
   * Commands are used to alter state machine state. All commands will be forwarded to the current cluster leader.
   * Once a leader receives the command, it will write the command to its internal {@code Log} and replicate it to a majority
   * of the cluster. Once the command has been replicated to a majority of the cluster, it will apply the command to its
   * {@code StateMachine} and respond with the result.
   * <p>
   * Once the command has been applied to a server state machine, the returned {@link CompletableFuture}
   * will be completed with the state machine output.
   * <p>
   * Note that all client submissions are guaranteed to be completed in the same order in which they were sent (program order)
   * and on the same thread. This does not, however, mean that they'll be applied to the server-side replicated state machine
   * in that order.
   *
   * @param command The command to submit.
   * @param <T> The command result type.
   * @return A completable future to be completed with the command result. The future is guaranteed to be completed after all
   * {@link Command} or {@link Query} submission futures that preceded it. The future will always be completed on the
   * @throws NullPointerException if {@code command} is null
   */
  <T> CompletableFuture<T> submit(Command<T> command);

  /**
   * Submits a query to the Copycat cluster.
   * <p>
   * Queries are used to read state machine state. The behavior of query submissions is primarily dependent on the
   * query's {@link Query.ConsistencyLevel}. For {@link Query.ConsistencyLevel#LINEARIZABLE}
   * and {@link Query.ConsistencyLevel#LINEARIZABLE_LEASE} consistency levels, queries will be forwarded
   * to the cluster leader. For lower consistency levels, queries are allowed to read from followers. All queries are executed
   * by applying queries to an internal server state machine.
   * <p>
   * Once the query has been applied to a server state machine, the returned {@link CompletableFuture}
   * will be completed with the state machine output.
   *
   * @param query The query to submit.
   * @param <T> The query result type.
   * @return A completable future to be completed with the query result. The future is guaranteed to be completed after all
   * {@link Command} or {@link Query} submission futures that preceded it. The future will always be completed on the
   * @throws NullPointerException if {@code query} is null
   */
  <T> CompletableFuture<T> submit(Query<T> query);

  /**
   * Registers a void event listener.
   * <p>
   * The registered {@link Runnable} will be {@link Runnable#run() called} when an event is received
   * from the Raft cluster for the client. {@link CopycatClient} implementations must guarantee that consumers are
   * always called in the same thread for the session. Therefore, no two events will be received concurrently
   * by the session. Additionally, events are guaranteed to be received in the order in which they were sent by
   * the state machine.
   *
   * @param event The event to which to listen.
   * @param callback The session receive callback.
   * @return The listener context.
   * @throws NullPointerException if {@code event} or {@code callback} is null
   */
  Listener<Void> onEvent(String event, Runnable callback);

  /**
   * Registers an event listener.
   * <p>
   * The registered {@link Consumer} will be {@link Consumer#accept(Object) called} when an event is received
   * from the Raft cluster for the session. {@link CopycatClient} implementations must guarantee that consumers are
   * always called in the same thread for the session. Therefore, no two events will be received concurrently
   * by the session. Additionally, events are guaranteed to be received in the order in which they were sent by
   * the state machine.
   *
   * @param event The event to which to listen.
   * @param callback The session receive callback.
   * @param <T> The session event type.
   * @return The listener context.
   * @throws NullPointerException if {@code event} or {@code callback} is null
   */
  <T> Listener<T> onEvent(String event, Consumer<T> callback);

  /**
   * Returns a boolean indicating whether the session is open.
   *
   * @return Indicates whether the session is open.
   */
  boolean isOpen();

  /**
   * Closes the session.
   *
   * @return A completable future to be completed once the session is closed.
   */
  CompletableFuture<Void> close();

  /**
   * Copycat session builder.
   */
  abstract class Builder implements io.atomix.catalyst.util.Builder<CopycatSession> {
    protected String name;
    protected String type;
    protected CommunicationStrategy communicationStrategy = CommunicationStrategies.LEADER;

    /**
     * Sets the session name.
     *
     * @param name The session name.
     * @return The session builder.
     */
    public Builder withName(String name) {
      this.name = Assert.notNull(name, "name");
      return this;
    }

    /**
     * Sets the session type.
     *
     * @param type The session type.
     * @return The session builder.
     */
    public Builder withType(String type) {
      this.type = Assert.notNull(type, "type");
      return this;
    }

    /**
     * Sets the session's communication strategy.
     *
     * @param communicationStrategy The session's communication strategy.
     * @return The session builder.
     * @throws NullPointerException if the communication strategy is null
     */
    public Builder withCommunicationStrategy(CommunicationStrategy communicationStrategy) {
      this.communicationStrategy = Assert.notNull(communicationStrategy, "communicationStrategy");
      return this;
    }
  }

}
