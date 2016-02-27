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

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.copycat.Command;
import io.atomix.copycat.protocol.PublishRequest;
import io.atomix.copycat.protocol.PublishResponse;
import io.atomix.copycat.protocol.Response;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.session.Event;
import io.atomix.copycat.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Raft session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ServerSessionContext implements ServerSession {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerSessionContext.class);
  private final long id;
  private final UUID client;
  private final Log log;
  private final ServerStateMachineContext context;
  private boolean open;
  private volatile State state = State.OPEN;
  private final long timeout;
  private Connection connection;
  private Address address;
  private volatile long references;
  private long connectIndex;
  private long keepAliveIndex;
  private long requestSequence;
  private long commandSequence;
  private long lastApplied;
  private long commandLowWaterMark;
  private long eventIndex;
  private long completeIndex;
  private long closeIndex;
  private long timestamp;
  private final Queue<List<Runnable>> queriesPool = new ArrayDeque<>();
  private final Map<Long, List<Runnable>> sequenceQueries = new HashMap<>();
  private final Map<Long, List<Runnable>> indexQueries = new HashMap<>();
  private final Map<Long, Runnable> commands = new HashMap<>();
  private final Map<Long, Object> responses = new HashMap<>();
  private final Queue<EventHolder> events = new ArrayDeque<>();
  private EventHolder event;
  private final Map<Long, CompletableFuture<Void>> futures = new HashMap<>();
  private boolean unregistering;
  private final Listeners<State> changeListeners = new Listeners<>();

  ServerSessionContext(long id, UUID client, Log log, ServerStateMachineContext context, long timeout) {
    this.id = id;
    this.client = Assert.notNull(client, "client");
    this.log = Assert.notNull(log, "log");
    this.eventIndex = id;
    this.completeIndex = id;
    this.lastApplied = id - 1;
    this.context = context;
    this.timeout = timeout;
  }

  @Override
  public long id() {
    return id;
  }

  /**
   * Returns the session client ID.
   *
   * @return The session client ID.
   */
  public UUID client() {
    return client;
  }

  /**
   * Opens the session.
   */
  void open() {
    open = true;
  }

  @Override
  public State state() {
    return state;
  }

  /**
   * Updates the session state.
   *
   * @param state The session state.
   */
  private void setState(State state) {
    if (this.state != state) {
      this.state = state;
      LOGGER.debug("{} - State changed: {}", id, state);
      changeListeners.forEach(l -> l.accept(state));
    }
  }

  @Override
  public Listener<State> onStateChange(Consumer<State> callback) {
    return changeListeners.add(callback);
  }

  /**
   * Acquires a reference to the session.
   */
  void acquire() {
    references++;
  }

  /**
   * Releases a reference to the session.
   */
  void release() {
    long references = --this.references;
    if (!state.active() && references == 0) {
      context.sessions().unregisterSession(id);
      log.release(id);
      if (closeIndex > 0) {
        log.release(closeIndex);
      }
    }
  }

  /**
   * Returns the number of open command references for the session.
   *
   * @return The number of open command references for the session.
   */
  long references() {
    return references;
  }

  /**
   * Returns the session timeout.
   *
   * @return The session timeout.
   */
  long timeout() {
    return timeout;
  }

  /**
   * Returns the session timestamp.
   *
   * @return The session timestamp.
   */
  long getTimestamp() {
    return timestamp;
  }

  /**
   * Sets the session timestamp.
   *
   * @param timestamp The session timestamp.
   * @return The server session.
   */
  ServerSessionContext setTimestamp(long timestamp) {
    this.timestamp = Math.max(this.timestamp, timestamp);
    return this;
  }

  /**
   * Returns the current session connect index.
   *
   * @return The current session connect index.
   */
  long getConnectIndex() {
    return connectIndex;
  }

  /**
   * Sets the current session connect index.
   *
   * @param connectIndex The current session connect index.
   * @return The server session.
   */
  ServerSessionContext setConnectIndex(long connectIndex) {
    long previousConnectIndex = this.connectIndex;
    this.connectIndex = connectIndex;
    if (previousConnectIndex > 0) {
      log.release(previousConnectIndex);
    }
    return this;
  }

  /**
   * Returns the current session keep alive index.
   *
   * @return The current session keep alive index.
   */
  long getKeepAliveIndex() {
    return keepAliveIndex;
  }

  /**
   * Sets the current session keep alive index.
   *
   * @param keepAliveIndex The current session keep alive index.
   * @return The server session.
   */
  ServerSessionContext setKeepAliveIndex(long keepAliveIndex) {
    long previousKeepAliveIndex = this.keepAliveIndex;
    this.keepAliveIndex = keepAliveIndex;
    if (previousKeepAliveIndex > 0) {
      log.release(previousKeepAliveIndex);
    }
    return this;
  }

  /**
   * Returns the session request number.
   *
   * @return The session request number.
   */
  long getRequestSequence() {
    return requestSequence;
  }

  /**
   * Returns the next session request number.
   *
   * @return The next session request number.
   */
  long nextRequestSequence() {
    return requestSequence + 1;
  }

  /**
   * Sets the session request number.
   *
   * @param request The session request number.
   * @return The server session.
   */
  ServerSessionContext setRequestSequence(long request) {
    if (request > this.requestSequence) {
      this.requestSequence = request;

      // When the request sequence number is incremented, get the next queued request callback and call it.
      // This will allow the command request to be evaluated in sequence.
      Runnable command = this.commands.remove(nextRequestSequence());
      if (command != null) {
        command.run();
      }
    }
    return this;
  }

  /**
   * Returns the session operation sequence number.
   *
   * @return The session operation sequence number.
   */
  long getCommandSequence() {
    return commandSequence;
  }

  /**
   * Returns the next operation sequence number.
   *
   * @return The next operation sequence number.
   */
  long nextCommandSequence() {
    return commandSequence + 1;
  }

  /**
   * Sets the session operation sequence number.
   *
   * @param sequence The session operation sequence number.
   * @return The server session.
   */
  ServerSessionContext setCommandSequence(long sequence) {
    // For each increment of the sequence number, trigger query callbacks that are dependent on the specific sequence.
    for (long i = commandSequence + 1; i <= sequence; i++) {
      commandSequence = i;
      List<Runnable> queries = this.sequenceQueries.remove(commandSequence);
      if (queries != null) {
        for (Runnable query : queries) {
          query.run();
        }
        queries.clear();
        queriesPool.add(queries);
      }
    }

    // If the request sequence number is less than the applied sequence number, update the request
    // sequence number. This is necessary to ensure that if the local server is a follower that is
    // later elected leader, its sequences are consistent for commands.
    if (sequence > requestSequence) {
      // Only attempt to trigger command callbacks if any are registered.
      if (!this.commands.isEmpty()) {
        // For each request sequence number, a command callback completing the command submission may exist.
        for (long i = this.requestSequence + 1; i <= requestSequence; i++) {
          this.requestSequence = i;
          Runnable command = this.commands.remove(i);
          if (command != null) {
            command.run();
          }
        }
      } else {
        this.requestSequence = sequence;
      }
    }

    return this;
  }

  /**
   * Returns the session index.
   *
   * @return The session index.
   */
  long getLastApplied() {
    return lastApplied;
  }

  /**
   * Sets the session index.
   *
   * @param index The session index.
   * @return The server session.
   */
  ServerSessionContext setLastApplied(long index) {
    // Query callbacks for this session are added to the indexQueries map to be executed once the required index
    // for the query is reached. For each increment of the index, trigger query callbacks that are dependent
    // on the specific index.
    for (long i = lastApplied + 1; i <= index; i++) {
      lastApplied = i;
      List<Runnable> queries = this.indexQueries.remove(lastApplied);
      if (queries != null) {
        for (Runnable query : queries) {
          query.run();
        }
        queries.clear();
        queriesPool.add(queries);
      }
    }

    return this;
  }

  /**
   * Adds a command to be executed in sequence.
   *
   * @param sequence The command sequence number.
   * @param runnable The command to execute.
   * @return The server session.
   */
  ServerSessionContext registerRequest(long sequence, Runnable runnable) {
    commands.put(sequence, runnable);
    return this;
  }

  /**
   * Registers a causal session query.
   *
   * @param sequence The session sequence number at which to execute the query.
   * @param query The query to execute.
   * @return The server session.
   */
  ServerSessionContext registerSequenceQuery(long sequence, Runnable query) {
    // Add a query to be run once the session's sequence number reaches the given sequence number.
    List<Runnable> queries = this.sequenceQueries.computeIfAbsent(sequence, v -> {
      List<Runnable> q = queriesPool.poll();
      return q != null ? q : new ArrayList<>(128);
    });
    queries.add(query);
    return this;
  }

  /**
   * Registers a session index query.
   *
   * @param index The state machine index at which to execute the query.
   * @param query The query to execute.
   * @return The server session.
   */
  ServerSessionContext registerIndexQuery(long index, Runnable query) {
    // Add a query to be run once the session's index reaches the given index.
    List<Runnable> queries = this.indexQueries.computeIfAbsent(index, v -> {
      List<Runnable> q = queriesPool.poll();
      return q != null ? q : new ArrayList<>(128);
    });
    queries.add(query);
    return this;
  }

  /**
   * Registers a session response.
   * <p>
   * Responses are stored in memory on all servers in order to provide linearizable semantics. When a command
   * is applied to the state machine, the command's return value is stored with the sequence number. Once the
   * client acknowledges receipt of the command output the response will be cleared from memory.
   *
   * @param sequence The response sequence number.
   * @param response The response.
   * @return The server session.
   */
  ServerSessionContext registerResponse(long sequence, Object response, CompletableFuture<Void> future) {
    responses.put(sequence, response);
    if (future != null)
      futures.put(sequence, future);
    return this;
  }

  /**
   * Clears command responses up to the given sequence number.
   * <p>
   * Command output is removed from memory up to the given sequence number. Additionally, since we know the
   * client received a response for all commands up to the given sequence number, command futures are removed
   * from memory as well.
   *
   * @param sequence The sequence to clear.
   * @return The server session.
   */
  ServerSessionContext clearResponses(long sequence) {
    if (sequence > commandLowWaterMark) {
      for (long i = commandLowWaterMark + 1; i <= sequence; i++) {
        responses.remove(i);
        futures.remove(i);
        commandLowWaterMark = i;
      }
    }
    return this;
  }

  /**
   * Returns the session response for the given sequence number.
   *
   * @param sequence The response sequence.
   * @return The response.
   */
  Object getResponse(long sequence) {
    return responses.get(sequence);
  }

  /**
   * Returns the response future for the given sequence.
   *
   * @param sequence The response sequence.
   * @return The response future.
   */
  CompletableFuture<Void> getResponseFuture(long sequence) {
    return futures.get(sequence);
  }

  /**
   * Sets the session connection.
   */
  ServerSessionContext setConnection(Connection connection) {
    this.connection = connection;
    return this;
  }

  /**
   * Returns the session connection.
   *
   * @return The session connection.
   */
  Connection getConnection() {
    return connection;
  }

  /**
   * Sets the session address.
   */
  ServerSessionContext setAddress(Address address) {
    this.address = address;
    return this;
  }

  /**
   * Returns the session address.
   */
  Address getAddress() {
    return address;
  }

  @Override
  public Session publish(String event) {
    return publish(event, null);
  }

  @Override
  public Session publish(String event, Object message) {
    Assert.state(open, "cannot publish events during session registration");
    Assert.stateNot(state == State.CLOSED, "session is closed");
    Assert.stateNot(state == State.EXPIRED, "session is expired");
    Assert.state(context.consistency() != null, "session events can only be published during command execution");

    // If the client acked an index greater than the current event sequence number since we know the
    // client must have received it from another server.
    if (completeIndex > context.index())
      return this;

    // If no event has been published for this index yet, create a new event holder.
    if (this.event == null || this.event.eventIndex != context.index()) {
      long previousIndex = eventIndex;
      eventIndex = context.index();
      this.event = new EventHolder(eventIndex, previousIndex);
    }

    // If the current context is LINEARIZABLE and no future has been set for the event, create one.
    if (context.consistency() == Command.ConsistencyLevel.LINEARIZABLE && this.event.future == null) {
      this.event.future = new CompletableFuture<>();
    }

    // Add the event to the event holder.
    this.event.events.add(new Event<>(event, message));

    return this;
  }

  /**
   * Commits events for the given index.
   */
  CompletableFuture<Void> commit(long index) {
    if (event != null && event.eventIndex == index) {
      events.add(event);
      sendEvent(event);
      return event.future;
    }
    return null;
  }

  /**
   * Returns the index of the highest event acked for the session.
   *
   * @return The index of the highest event acked for the session.
   */
  long getLastCompleted() {
    // If there are any queued events, return the index prior to the first event in the queue.
    EventHolder event = events.peek();
    if (event != null && event.eventIndex > completeIndex) {
      return event.eventIndex - 1;
    }
    // If no events are queued, return the highest index applied to the session.
    return lastApplied;
  }

  /**
   * Clears events up to the given sequence.
   *
   * @param index The index to clear.
   * @return The server session.
   */
  private ServerSessionContext clearEvents(long index) {
    if (index > completeIndex) {
      EventHolder event = events.peek();
      while (event != null && event.eventIndex <= index) {
        events.remove();
        completeIndex = event.eventIndex;
        if (event.future != null)
          event.future.complete(null);
        event = events.peek();
      }
      completeIndex = index;
    }
    return this;
  }

  /**
   * Resends events from the given sequence.
   *
   * @param index The index from which to resend events.
   * @return The server session.
   */
  ServerSessionContext resendEvents(long index) {
    clearEvents(index);
    for (EventHolder event : events) {
      sendSequentialEvent(event);
    }
    return this;
  }

  /**
   * Sends an event to the session.
   */
  private void sendEvent(EventHolder event) {
    // Linearizable events must be sent synchronously, so only send them within a synchronous context.
    if (context.synchronous() && context.consistency() == Command.ConsistencyLevel.LINEARIZABLE) {
      sendLinearizableEvent(event);
    } else if (context.consistency() != Command.ConsistencyLevel.LINEARIZABLE) {
      sendSequentialEvent(event);
    }
  }

  /**
   * Sends a linearizable event.
   */
  private void sendLinearizableEvent(EventHolder event) {
    if (connection != null) {
      sendEvent(event, connection);
    } else if (address != null) {
      context.connections().getConnection(address).thenAccept(connection -> sendEvent(event, connection));
    }
  }

  /**
   * Sends a sequential event.
   */
  private void sendSequentialEvent(EventHolder event) {
    if (connection != null) {
      sendEvent(event, connection);
    }
  }

  /**
   * Sends an event.
   */
  private void sendEvent(EventHolder event, Connection connection) {
    PublishRequest request = PublishRequest.builder()
      .withSession(id())
      .withEventIndex(event.eventIndex)
      .withPreviousIndex(event.previousIndex)
      .withEvents(event.events)
      .build();

    LOGGER.debug("{} - Sending {}", id, request);
    connection.<PublishRequest, PublishResponse>send(request).whenComplete((response, error) -> {
      if (error == null) {
        LOGGER.debug("{} - Received {}", id, response);
        // If the event was received successfully, clear events up to the event index.
        if (response.status() == Response.Status.OK) {
          clearEvents(response.index());
        }
        // If the event failed and the response index is non-null, resend all events from the response index.
        else if (response.error() == null && response.index() > 0) {
          resendEvents(response.index());
        }
      }
    });
  }

  /**
   * Sets the session as suspect.
   */
  void suspect() {
    setState(State.UNSTABLE);
  }

  /**
   * Sets the session as trusted.
   */
  void trust() {
    setState(State.OPEN);
  }

  /**
   * Sets the session as being unregistered.
   */
  void unregister() {
    unregistering = true;
  }

  /**
   * Indicates whether the session is being unregistered.
   */
  boolean isUnregistering() {
    return unregistering;
  }

  /**
   * Expires the session.
   *
   * @param index The index at which to expire the session.
   */
  void expire(long index) {
    cleanEvents();
    setState(State.EXPIRED);
    cleanState(index);
  }

  /**
   * Closes the session.
   *
   * @param index The index at which to close the session.
   */
  void close(long index) {
    cleanEvents();
    setState(State.CLOSED);
    cleanState(index);
  }

  /**
   * Cleans up session events on close.
   */
  private void cleanEvents() {
    for (EventHolder event : events) {
      if (event.future != null) {
        event.future.complete(null);
      }
    }
  }

  /**
   * Cleans session entries on close.
   */
  private void cleanState(long index) {
    // If the keep alive index is set, release the entry.
    if (keepAliveIndex > 0) {
      log.release(keepAliveIndex);
    }

    // If no references to session commands are open, release session-related entries.
    if (references == 0) {
      log.release(id);
      log.release(index);
      context.sessions().unregisterSession(id);
    } else {
      this.closeIndex = index;
    }
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + (int)(id ^ (id >>> 32));
    return hashCode;
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Session && ((Session) object).id() == id;
  }

  @Override
  public String toString() {
    return String.format("%s[id=%d]", getClass().getSimpleName(), id);
  }

  /**
   * Event holder.
   */
  private static class EventHolder {
    private final long eventIndex;
    private final long previousIndex;
    private final List<Event<?>> events = new ArrayList<>(8);
    private CompletableFuture<Void> future;

    private EventHolder(long eventIndex, long previousIndex) {
      this.eventIndex = eventIndex;
      this.previousIndex = previousIndex;
    }
  }

}
