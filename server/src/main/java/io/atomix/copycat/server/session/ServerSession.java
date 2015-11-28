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
package io.atomix.copycat.server.session;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.request.PublishRequest;
import io.atomix.copycat.client.response.PublishResponse;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.client.session.Event;
import io.atomix.copycat.client.session.Session;
import io.atomix.copycat.server.executor.ServerStateMachineContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Raft session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ServerSession implements Session {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServerSession.class);
  private final long id;
  private final ServerStateMachineContext context;
  private final long timeout;
  private Connection connection;
  private Address address;
  private long request;
  private long sequence;
  private long version;
  private long commandLowWaterMark;
  private long eventVersion;
  private long eventAckVersion;
  private long timestamp;
  private final Queue<List<Runnable>> queriesPool = new ArrayDeque<>();
  private final Map<Long, List<Runnable>> sequenceQueries = new HashMap<>();
  private final Map<Long, List<Runnable>> versionQueries = new HashMap<>();
  private final Map<Long, Runnable> commands = new HashMap<>();
  private final Map<Long, Object> responses = new HashMap<>();
  private final Queue<EventHolder> events = new ArrayDeque<>();
  private EventHolder event;
  private final Map<Long, CompletableFuture<Void>> futures = new HashMap<>();
  private boolean suspect;
  private boolean unregistering;
  private boolean expired;
  private boolean closed = true;
  private final Map<String, Listeners<Object>> eventListeners = new ConcurrentHashMap<>();
  private final Listeners<Session> openListeners = new Listeners<>();
  private final Listeners<Session> closeListeners = new Listeners<>();

  public ServerSession(long id, ServerStateMachineContext context, long timeout) {
    this.id = id;
    this.eventAckVersion = id;
    this.version = id - 1;
    this.context = context;
    this.timeout = timeout;
  }

  @Override
  public long id() {
    return id;
  }

  /**
   * Opens the session.
   */
  public void open() {
    closed = false;
  }

  /**
   * Returns the session timeout.
   *
   * @return The session timeout.
   */
  public long timeout() {
    return timeout;
  }

  /**
   * Returns the session timestamp.
   *
   * @return The session timestamp.
   */
  public long getTimestamp() {
    return timestamp;
  }

  /**
   * Sets the session timestamp.
   *
   * @param timestamp The session timestamp.
   * @return The server session.
   */
  public ServerSession setTimestamp(long timestamp) {
    this.timestamp = Math.max(this.timestamp, timestamp);
    return this;
  }

  /**
   * Returns the session request number.
   *
   * @return The session request number.
   */
  public long getRequest() {
    return request;
  }

  /**
   * Returns the next session request number.
   *
   * @return The next session request number.
   */
  public long nextRequest() {
    return request + 1;
  }

  /**
   * Sets the session request number.
   *
   * @param request The session request number.
   * @return The server session.
   */
  public ServerSession setRequest(long request) {
    if (request > this.request) {
      this.request = request;

      // When the request sequence number is incremented, get the next queued request callback and call it.
      // This will allow the command request to be evaluated in sequence.
      Runnable command = this.commands.remove(nextRequest());
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
  public long getSequence() {
    return sequence;
  }

  /**
   * Returns the next operation sequence number.
   *
   * @return The next operation sequence number.
   */
  public long nextSequence() {
    return sequence + 1;
  }

  /**
   * Sets the session operation sequence number.
   *
   * @param sequence The session operation sequence number.
   * @return The server session.
   */
  public ServerSession setSequence(long sequence) {
    // For each increment of the sequence number, trigger query callbacks that are dependent on the specific sequence.
    for (long i = this.sequence + 1; i <= sequence; i++) {
      this.sequence = i;
      List<Runnable> queries = this.sequenceQueries.remove(this.sequence);
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
    if (sequence > request) {
      // Only attempt to trigger command callbacks if any are registered.
      if (!this.commands.isEmpty()) {
        // For each request sequence number, a command callback completing the command submission may exist.
        for (long i = this.request + 1; i <= request; i++) {
          this.request = i;
          Runnable command = this.commands.remove(i);
          if (command != null) {
            command.run();
          }
        }
      } else {
        this.request = sequence;
      }
    }

    return this;
  }

  /**
   * Returns the session version.
   *
   * @return The session version.
   */
  public long getVersion() {
    return version;
  }

  /**
   * Sets the session version.
   *
   * @param version The session version.
   * @return The server session.
   */
  public ServerSession setVersion(long version) {
    // Query callbacks for this session are added to the versionQueries map to be executed once the required version
    // for the query is reached. For each increment of the version number, trigger query callbacks that are dependent
    // on the specific version.
    for (long i = this.version + 1; i <= version; i++) {
      this.version = i;
      List<Runnable> queries = this.versionQueries.remove(this.version);
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
  public ServerSession registerRequest(long sequence, Runnable runnable) {
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
  public ServerSession registerSequenceQuery(long sequence, Runnable query) {
    // Add a query to be run once the session's sequence number reaches the given sequence number.
    List<Runnable> queries = this.sequenceQueries.computeIfAbsent(sequence, v -> {
      List<Runnable> q = queriesPool.poll();
      return q != null ? q : new ArrayList<>(128);
    });
    queries.add(query);
    return this;
  }

  /**
   * Registers a sequential session query.
   *
   * @param version The state machine version (index) at which to execute the query.
   * @param query The query to execute.
   * @return The server session.
   */
  public ServerSession registerVersionQuery(long version, Runnable query) {
    // Add a query to be run once the session's version number reaches the given version number.
    List<Runnable> queries = this.versionQueries.computeIfAbsent(version, v -> {
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
  public ServerSession registerResponse(long sequence, Object response, CompletableFuture<Void> future) {
    responses.put(sequence, response);
    if (future != null)
      futures.put(sequence, future);
    return this;
  }

  /**
   * Clears command responses up to the given version.
   * <p>
   * Command output is removed from memory up to the given sequence number. Additionally, since we know the
   * client received a response for all commands up to the given sequence number, command futures are removed
   * from memory as well.
   *
   * @param sequence The sequence to clear.
   * @return The server session.
   */
  public ServerSession clearResponses(long sequence) {
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
   * Returns the session response for the given version.
   *
   * @param sequence The response sequence.
   * @return The response.
   */
  public Object getResponse(long sequence) {
    return responses.get(sequence);
  }

  /**
   * Returns the response future for the given sequence.
   *
   * @param sequence The response sequence.
   * @return The response future.
   */
  public CompletableFuture<Void> getResponseFuture(long sequence) {
    return futures.get(sequence);
  }

  /**
   * Sets the session connection.
   */
  public ServerSession setConnection(Connection connection) {
    this.connection = connection;
    if (connection != null) {
      connection.handler(PublishRequest.class, this::handlePublish);
    }
    return this;
  }

  /**
   * Returns the session connection.
   *
   * @return The session connection.
   */
  public Connection getConnection() {
    return connection;
  }

  /**
   * Sets the session address.
   */
  public ServerSession setAddress(Address address) {
    this.address = address;
    return this;
  }

  /**
   * Returns the session address.
   */
  public Address getAddress() {
    return address;
  }

  @Override
  public Session publish(String event) {
    return publish(event, null);
  }

  @Override
  public Session publish(String event, Object message) {
    Assert.stateNot(closed, "session is not open");
    Assert.state(context.consistency() != null, "session events can only be published during command execution");

    // If the client acked a version greater than the current event sequence number since we know the client must have received it from another server.
    if (eventAckVersion > context.version())
      return this;

    // If no event has been published for this version yet, create a new event holder.
    if (this.event == null || this.event.eventVersion != context.version()) {
      long previousVersion = eventVersion;
      eventVersion = context.version();
      this.event = new EventHolder(eventVersion, previousVersion);
    }

    // Add the event to the event holder.
    this.event.events.add(new Event<>(event, message));

    return this;
  }

  /**
   * Commits events for the given version.
   */
  public CompletableFuture<Void> commit(long version) {
    if (event != null && event.eventVersion == version) {
      events.add(event);
      sendEvent(event);
      return event.future;
    }
    return null;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Listener<Void> onEvent(String event, Runnable callback) {
    return onEvent(event, v -> callback.run());
  }

  @Override
  @SuppressWarnings("unchecked")
  public Listener onEvent(String event, Consumer listener) {
    return eventListeners.computeIfAbsent(Assert.notNull(event, "event"), e -> new Listeners<>())
      .add(Assert.notNull(listener, "listener"));
  }

  /**
   * Returns the index of the highest event acked for the session.
   *
   * @return The index of the highest event acked for the session.
   */
  public long getLastCompleted() {
    // If there are any queued events, return the index prior to the first event in the queue.
    EventHolder event = events.peek();
    if (event != null && event.eventVersion > eventAckVersion) {
      return event.eventVersion - 1;
    }
    // If no events are queued, return the highest index applied to the session.
    return version;
  }

  /**
   * Clears events up to the given sequence.
   *
   * @param version The version to clear.
   * @return The server session.
   */
  private ServerSession clearEvents(long version) {
    if (version > eventAckVersion) {
      EventHolder event = events.peek();
      while (event != null && event.eventVersion <= version) {
        events.remove();
        eventAckVersion = event.eventVersion;
        event.future.complete(null);
        event = events.peek();
      }
      eventAckVersion = version;
    }
    return this;
  }

  /**
   * Resends events from the given sequence.
   *
   * @param version The version from which to resend events.
   * @return The server session.
   */
  public ServerSession resendEvents(long version) {
    if (version > eventAckVersion) {
      clearEvents(version);
      for (EventHolder event : events) {
        sendSequentialEvent(event);
      }
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
      .withEventVersion(event.eventVersion)
      .withPreviousVersion(Math.max(event.previousVersion, eventAckVersion))
      .withEvents(event.events)
      .build();

    LOGGER.debug("{} - Sending {}", id, request);
    connection.<PublishRequest, PublishResponse>send(request).whenComplete((response, error) -> {
      if (isOpen() && error == null) {
        if (response.status() == Response.Status.OK) {
          clearEvents(response.version());
        } else if (response.error() == null) {
          resendEvents(response.version());
        }
      }
    });
  }

  /**
   * Handles a publish request.
   *
   * @param request The publish request to handle.
   * @return A completable future to be completed with the publish response.
   */
  @SuppressWarnings("unchecked")
  protected CompletableFuture<PublishResponse> handlePublish(PublishRequest request) {
    for (Event<?> event : request.events()) {
      Listeners<Object> listeners = eventListeners.get(event.name());
      if (listeners != null) {
        for (Listener listener : listeners) {
          listener.accept(event.message());
        }
      }
    }

    return CompletableFuture.completedFuture(PublishResponse.builder()
      .withStatus(Response.Status.OK)
      .build());
  }

  @Override
  public boolean isOpen() {
    return !closed;
  }

  @Override
  public Listener<Session> onOpen(Consumer<Session> listener) {
    return openListeners.add(Assert.notNull(listener, "listener"));
  }

  /**
   * Closes the session.
   */
  public void close() {
    closed = true;
    for (Listener<Session> listener : closeListeners) {
      listener.accept(this);
    }
  }

  @Override
  public Listener<Session> onClose(Consumer<Session> listener) {
    Listener<Session> context = closeListeners.add(Assert.notNull(listener, "listener"));
    if (closed) {
      context.accept(this);
    }
    return context;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  /**
   * Sets the session as suspect.
   */
  public void suspect() {
    suspect = true;
  }

  /**
   * Sets the session as trusted.
   */
  public void trust() {
    suspect = false;
  }

  /**
   * Indicates whether the session is suspect.
   */
  public boolean isSuspect() {
    return suspect;
  }

  /**
   * Sets the session as being unregistered.
   */
  public void unregister() {
    unregistering = true;
  }

  /**
   * Indicates whether the session is being unregistered.
   */
  public boolean isUnregistering() {
    return unregistering;
  }

  /**
   * Expires the session.
   */
  public void expire() {
    closed = true;
    expired = true;
    for (EventHolder event : events) {
      event.future.complete(null);
    }
    for (Listener<Session> listener : closeListeners) {
      listener.accept(this);
    }
  }

  @Override
  public boolean isExpired() {
    return expired;
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
    private final long eventVersion;
    private final long previousVersion;
    private final List<Event<?>> events = new ArrayList<>(8);
    private final CompletableFuture<Void> future = new CompletableFuture<>();

    private EventHolder(long eventVersion, long previousVersion) {
      this.eventVersion = eventVersion;
      this.previousVersion = previousVersion;
    }
  }

}
