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
import io.atomix.copycat.protocol.PublishResponse;
import io.atomix.copycat.protocol.Response;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.storage.Log;
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
  private final String client;
  private final Log log;
  private final ServerStateMachineContext context;
  private boolean open;
  private volatile State state = State.OPEN;
  private final long timeout;
  private Connection connection;
  private volatile long references;
  private long keepAliveIndex;
  private long requestSequence;
  private long commandSequence;
  private long lastApplied;
  private long commandLowWaterMark;
  private long eventIndex;
  private long completeIndex;
  private long closeIndex;
  private long timestamp;
  private final Map<Long, List<Runnable>> sequenceQueries = new HashMap<>();
  private final Map<Long, List<Runnable>> indexQueries = new HashMap<>();
  private final Map<Long, ServerStateMachine.Result> results = new HashMap<>();
  private final Queue<EventHolder> events = new LinkedList<>();
  private EventHolder event;
  private boolean unregistering;
  private final Listeners<State> changeListeners = new Listeners<>();

  ServerSessionContext(long id, String client, Log log, ServerStateMachineContext context, long timeout) {
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
  public String client() {
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
   * @return The server session context.
   */
  ServerSessionContext resetRequestSequence(long requestSequence) {
    this.requestSequence = requestSequence;
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
      }
    }

    // If the request sequence number is less than the applied sequence number, update the request
    // sequence number. This is necessary to ensure that if the local server is a follower that is
    // later elected leader, its sequences are consistent for commands.
    if (sequence > requestSequence) {
      this.requestSequence = sequence;
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
      }
    }

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
    List<Runnable> queries = this.sequenceQueries.computeIfAbsent(sequence, v -> new LinkedList<Runnable>());
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
    List<Runnable> queries = this.indexQueries.computeIfAbsent(index, v -> new LinkedList<>());
    queries.add(query);
    return this;
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
   * @return The server session.
   */
  ServerSessionContext registerResult(long sequence, ServerStateMachine.Result result) {
    results.put(sequence, result);
    return this;
  }

  /**
   * Clears command results up to the given sequence number.
   * <p>
   * Command output is removed from memory up to the given sequence number. Additionally, since we know the
   * client received a response for all commands up to the given sequence number, command futures are removed
   * from memory as well.
   *
   * @param sequence The sequence to clear.
   * @return The server session.
   */
  ServerSessionContext clearResults(long sequence) {
    if (sequence > commandLowWaterMark) {
      for (long i = commandLowWaterMark + 1; i <= sequence; i++) {
        results.remove(i);
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
  ServerStateMachine.Result getResult(long sequence) {
    return results.get(sequence);
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
    Assert.state(open, "cannot publish events during session registration");
    Assert.stateNot(state == State.CLOSED, "session is closed");
    Assert.stateNot(state == State.EXPIRED, "session is expired");
    Assert.state(context.type() == ServerStateMachineContext.Type.COMMAND, "session events can only be published during command execution");

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
    connection.<PublishRequest, PublishResponse>send(request).whenComplete((response, error) -> {
      if (error == null) {
        LOGGER.trace("{} - Received {}", id, response);
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
    setState(State.EXPIRED);
    cleanState(index);
  }

  /**
   * Closes the session.
   *
   * @param index The index at which to close the session.
   */
  void close(long index) {
    setState(State.CLOSED);
    cleanState(index);
  }

  /**
   * Cleans session entries on close.
   */
  private void cleanState(long index) {
    // If the keep alive index is set, release the entry.
    if (keepAliveIndex > 0) {
      log.release(keepAliveIndex);
    }

    context.sessions().unregisterSession(id);

    // If no references to session commands are open, release session-related entries.
    if (references == 0) {
      log.release(id);
      if (index > 0) {
        log.release(index);
      }
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
    private final List<Event<?>> events = new LinkedList<>();

    private EventHolder(long eventIndex, long previousIndex) {
      this.eventIndex = eventIndex;
      this.previousIndex = previousIndex;
    }
  }

}
