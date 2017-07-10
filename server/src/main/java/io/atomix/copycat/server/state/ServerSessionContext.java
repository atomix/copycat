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

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.Listeners;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.PublishRequest;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.LogCleaner;
import io.atomix.copycat.session.Event;
import io.atomix.copycat.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;

/**
 * Raft session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ServerSessionContext implements ServerSession {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerSessionContext.class);
  private final long id;
  private final String name;
  private final String type;
  private final long client;
  private final ServerStateMachineExecutor executor;
  private volatile State state = State.OPEN;
  private Connection connection;
  private final String messageType;
  private long requestSequence;
  private long commandSequence;
  private long lastApplied;
  private long commandLowWaterMark;
  private long eventIndex;
  private long completeIndex;
  private final Map<Long, List<Runnable>> sequenceQueries = new HashMap<>();
  private final Map<Long, List<Runnable>> indexQueries = new HashMap<>();
  private final Map<Long, OperationResult> results = new HashMap<>();
  private final Queue<EventHolder> events = new LinkedList<>();
  private EventHolder event;
  private final Listeners<State> changeListeners = new Listeners<>();

  ServerSessionContext(long id, String name, String type, long client, ServerStateMachineExecutor executor) {
    this.id = id;
    this.name = name;
    this.type = type;
    this.client = client;
    this.eventIndex = id;
    this.completeIndex = id;
    this.lastApplied = id;
    this.executor = executor;
    this.messageType = String.valueOf(id);
  }

  @Override
  public long id() {
    return id;
  }

  /**
   * Returns the session name.
   *
   * @return The session name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the session type.
   *
   * @return The session type.
   */
  public String type() {
    return type;
  }

  /**
   * Returns the session client ID.
   *
   * @return The session client ID.
   */
  public long client() {
    return client;
  }

  /**
   * Returns the state machine executor associated with the session.
   *
   * @return The state machine executor associated with the session.
   */
  ServerStateMachineExecutor getStateMachineExecutor() {
    return executor;
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
   * Returns the session request number.
   *
   * @return The session request number.
   */
  long getRequestSequence() {
    return requestSequence;
  }

  /**
   * Checks and sets the current request sequence number.
   *
   * @param requestSequence The request sequence number to set.
   * @return Indicates whether the given {@code requestSequence} number is the next sequence number.
   */
  boolean setRequestSequence(long requestSequence) {
    if (requestSequence == this.requestSequence + 1) {
      this.requestSequence = requestSequence;
      return true;
    } else if (requestSequence <= this.requestSequence) {
      return true;
    }
    return false;
  }

  /**
   * Resets the current request sequence number.
   *
   * @param requestSequence The request sequence number.
   */
  void resetRequestSequence(long requestSequence) {
    // If the request sequence number is less than the applied sequence number, update the request
    // sequence number. This is necessary to ensure that if the local server is a follower that is
    // later elected leader, its sequences are consistent for commands.
    if (requestSequence > this.requestSequence) {
      this.requestSequence = requestSequence;
    }
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
   */
  void setCommandSequence(long sequence) {
    // For each increment of the sequence number, trigger query callbacks that are dependent on the specific sequence.
    for (long i = commandSequence + 1; i <= sequence; i++) {
      commandSequence = i;
      List<Runnable> queries = this.sequenceQueries.remove(commandSequence);
      if (queries != null) {
        for (Runnable query : queries) {
          query.run();
        }
      }
    }
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
   */
  void setLastApplied(long index) {
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
      }
    }
  }

  /**
   * Registers a causal session query.
   *
   * @param sequence The session sequence number at which to execute the query.
   * @param query The query to execute.
   */
  void registerSequenceQuery(long sequence, Runnable query) {
    // Add a query to be run once the session's sequence number reaches the given sequence number.
    List<Runnable> queries = this.sequenceQueries.computeIfAbsent(sequence, v -> new LinkedList<Runnable>());
    queries.add(query);
  }

  /**
   * Registers a session index query.
   *
   * @param index The state machine index at which to execute the query.
   * @param query The query to execute.
   */
  void registerIndexQuery(long index, Runnable query) {
    // Add a query to be run once the session's index reaches the given index.
    List<Runnable> queries = this.indexQueries.computeIfAbsent(index, v -> new LinkedList<>());
    queries.add(query);
  }

  /**
   * Registers a session result.
   * <p>
   * Results are stored in memory on all servers in order to provide linearizable semantics. When a command
   * is applied to the state machine, the command's return value is stored with the sequence number. Once the
   * client acknowledges receipt of the command output the result will be cleared from memory.
   *
   * @param sequence The result sequence number.
   * @param result The result.
   */
  void registerResult(long sequence, OperationResult result) {
    results.put(sequence, result);
  }

  /**
   * Clears command results up to the given sequence number.
   * <p>
   * Command output is removed from memory up to the given sequence number. Additionally, since we know the
   * client received a response for all commands up to the given sequence number, command futures are removed
   * from memory as well.
   *
   * @param sequence The sequence to clear.
   */
  void clearResults(long sequence) {
    if (sequence > commandLowWaterMark) {
      for (long i = commandLowWaterMark + 1; i <= sequence; i++) {
        results.remove(i);
        commandLowWaterMark = i;
      }
    }
  }

  /**
   * Returns the session response for the given sequence number.
   *
   * @param sequence The response sequence.
   * @return The response.
   */
  OperationResult getResult(long sequence) {
    return results.get(sequence);
  }

  /**
   * Sets the session connection.
   */
  void setConnection(Connection connection) {
    this.connection = connection;
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
   * Returns the session event index.
   *
   * @return The session event index.
   */
  long getEventIndex() {
    return eventIndex;
  }

  @Override
  public Session publish(String event) {
    return publish(event, null);
  }

  @Override
  public Session publish(String event, Object message) {
    // Store volatile state in a local variable.
    State state = this.state;
    Assert.stateNot(state == State.CLOSED, "session is closed");
    Assert.stateNot(state == State.EXPIRED, "session is expired");
    Assert.state(executor.context().type() == ServerStateMachineContext.Type.COMMAND, "session events can only be published during command execution");

    // If the client acked an index greater than the current event sequence number since we know the
    // client must have received it from another server.
    if (completeIndex > executor.context().index()) {
      return this;
    }

    // If no event has been published for this index yet, create a new event holder.
    if (this.event == null || this.event.eventIndex != executor.context().index()) {
      long previousIndex = eventIndex;
      eventIndex = executor.context().index();
      this.event = new EventHolder(eventIndex, previousIndex);
    }

    // Add the event to the event holder.
    this.event.events.add(new Event<>(event, message));

    return this;
  }

  /**
   * Commits events for the given index.
   */
  void commit(long index) {
    if (event != null && event.eventIndex == index) {
      events.add(event);
      sendEvent(event);
    }
    setLastApplied(index);
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
      sendEvent(event);
    }
    return this;
  }

  /**
   * Sends an event to the session.
   */
  private void sendEvent(EventHolder event) {
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
      .withPreviousIndex(Math.max(event.previousIndex, completeIndex))
      .withEvents(event.events)
      .build();

    LOGGER.trace("{} - Sending {}", id, request);
    connection.send(messageType, request);
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
    private final List<Event<?>> events = new LinkedList<>();

    private EventHolder(long eventIndex, long previousIndex) {
      this.eventIndex = eventIndex;
      this.previousIndex = previousIndex;
    }
  }

}
