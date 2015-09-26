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
package io.atomix.catalogue.server.state;

import io.atomix.catalogue.client.request.PublishRequest;
import io.atomix.catalogue.client.response.PublishResponse;
import io.atomix.catalogue.client.response.Response;
import io.atomix.catalogue.client.session.Session;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.Listeners;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Raft session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ServerSession implements Session {
  private final long id;
  private final UUID connectionId;
  private final ServerStateMachineContext context;
  private final long timeout;
  private Connection connection;
  private long request;
  private long sequence;
  private long version;
  private long commandLowWaterMark;
  private long eventVersion;
  private long eventSequence;
  private long eventAckVersion;
  private long timestamp;
  private final Queue<List<Runnable>> queriesPool = new ArrayDeque<>();
  private final Map<Long, List<Runnable>> sequenceQueries = new HashMap<>();
  private final Map<Long, List<Runnable>> versionQueries = new HashMap<>();
  private final Map<Long, Runnable> commands = new HashMap<>();
  private final Map<Long, Object> responses = new HashMap<>();
  private final Queue<EventHolder> events = new ArrayDeque<>();
  private final Queue<EventHolder> eventsPool = new ArrayDeque<>();
  private boolean suspect;
  private boolean unregistering;
  private boolean expired;
  private boolean closed;
  private final Map<String, Listeners<Object>> eventListeners = new ConcurrentHashMap<>();
  private final Listeners<Session> openListeners = new Listeners<>();
  private final Listeners<Session> closeListeners = new Listeners<>();

  ServerSession(long id, UUID connectionId, ServerStateMachineContext context, long timeout) {
    if (connectionId == null)
      throw new NullPointerException("connection cannot be null");

    this.id = id;
    this.version = id - 1;
    this.connectionId = connectionId;
    this.context = context;
    this.timeout = timeout;
  }

  @Override
  public long id() {
    return id;
  }

  /**
   * Returns the server connection ID.
   *
   * @return The server connection ID.
   */
  UUID connection() {
    return connectionId;
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
  ServerSession setTimestamp(long timestamp) {
    this.timestamp = Math.max(this.timestamp, timestamp);
    return this;
  }

  /**
   * Returns the session request number.
   *
   * @return The session request number.
   */
  long getRequest() {
    return request;
  }

  /**
   * Returns the next session request number.
   *
   * @return The next session request number.
   */
  long nextRequest() {
    return request + 1;
  }

  /**
   * Sets the session request number.
   *
   * @param request The session request number.
   * @return The server session.
   */
  ServerSession setRequest(long request) {
    this.request = request;
    Runnable command = this.commands.remove(nextRequest());
    if (command != null) {
      command.run();
    }
    return this;
  }

  /**
   * Returns the session operation sequence number.
   *
   * @return The session operation sequence number.
   */
  long getSequence() {
    return sequence;
  }

  /**
   * Returns the next operation sequence number.
   *
   * @return The next operation sequence number.
   */
  long nextSequence() {
    return sequence + 1;
  }

  /**
   * Sets the session operation sequence number.
   *
   * @param sequence The session operation sequence number.
   * @return The server session.
   */
  ServerSession setSequence(long sequence) {
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

    return this;
  }

  /**
   * Returns the session version.
   *
   * @return The session version.
   */
  long getVersion() {
    return version;
  }

  /**
   * Returns the next session version.
   *
   * @return The next session version.
   */
  long nextVersion() {
    return version + 1;
  }

  /**
   * Sets the session version.
   *
   * @param version The session version.
   * @return The server session.
   */
  ServerSession setVersion(long version) {
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
  ServerSession registerRequest(long sequence, Runnable runnable) {
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
  ServerSession registerSequenceQuery(long sequence, Runnable query) {
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
  ServerSession registerVersionQuery(long version, Runnable query) {
    List<Runnable> queries = this.versionQueries.computeIfAbsent(version, v -> {
      List<Runnable> q = queriesPool.poll();
      return q != null ? q : new ArrayList<>(128);
    });
    queries.add(query);
    return this;
  }

  /**
   * Registers a session response.
   *
   * @param sequence The response sequence number.
   * @param response The response.
   * @return The server session.
   */
  ServerSession registerResponse(long sequence, Object response) {
    responses.put(sequence, response);
    return this;
  }

  /**
   * Clears command responses up to the given version.
   *
   * @param sequence The sequence to clear.
   * @return The server session.
   */
  ServerSession clearResponses(long sequence) {
    if (sequence > commandLowWaterMark) {
      for (long i = commandLowWaterMark + 1; i <= sequence; i++) {
        responses.remove(i);
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
  Object getResponse(long sequence) {
    return responses.get(sequence);
  }

  /**
   * Sets the session connection.
   */
  ServerSession setConnection(Connection connection) {
    this.connection = connection;
    if (connection != null) {
      if (!connection.id().equals(connectionId)) {
        throw new IllegalArgumentException("connection must match session connection ID");
      }
      connection.handler(PublishRequest.class, this::handlePublish);
    }
    return this;
  }

  @Override
  public Session publish(String event) {
    return publish(event, null);
  }

  @Override
  public Session publish(String event, Object message) {
    if (context.type() == ServerStateMachineContext.Type.QUERY)
      throw new IllegalStateException("cannot publish session events during query execution");

    // If the client acked a version greater than the current state machine version then immediately return.
    if (eventAckVersion > context.version())
      return this;

    // The previous event version and sequence are the current event version and sequence.
    long previousVersion = eventVersion;
    long previousSequence = eventSequence;

    // if the event version is not the current context version, reset the event version and sequence. Otherwise,
    // this must not be the first event, so increment the event sequence number.
    if (eventVersion != context.version()) {
      eventVersion = context.version();
      eventSequence = 1;
    } else {
      eventSequence++;
    }

    // Get a holder from the event holder pool.
    EventHolder holder = eventsPool.poll();
    if (holder == null)
      holder = new EventHolder();

    // Populate the event holder and add it to the events queue.
    holder.eventVersion = eventVersion;
    holder.eventSequence = eventSequence;
    holder.previousVersion = previousVersion;
    holder.previousSequence = previousSequence;
    holder.event = event;
    holder.message = message;

    // If this event was published by a command execution, create and register an event future.
    if (context.type() == ServerStateMachineContext.Type.COMMAND) {
      holder.future = new CompletableFuture<>();
      context.register(holder.future);
    } else {
      holder.future = null;
    }

    // Add the event holder to the events list. This will be used to ack events once completed.
    events.add(holder);

    // Send the event.
    sendEvent(holder);

    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Listener onEvent(String event, Consumer listener) {
    return eventListeners.computeIfAbsent(Assert.notNull(event, "event"), e -> new Listeners<>())
      .add(Assert.notNull(listener, "listener"));
  }

  /**
   * Clears events up to the given sequence.
   *
   * @param version The version to clear.
   * @param sequence The sequence to clear.
   * @return The server session.
   */
  ServerSession clearEvents(long version, long sequence) {
    if (version >= eventAckVersion) {
      eventAckVersion = version;

      EventHolder holder = events.peek();
      while (holder != null && (holder.eventVersion < version || (holder.eventVersion == version && holder.eventSequence <= sequence))){
        events.remove();
        if (holder.future != null)
          holder.future.complete(null);
        eventsPool.add(holder);
      }
    }
    return this;
  }

  /**
   * Resends events from the given sequence.
   *
   * @param version The version from which to resend events.
   * @param sequence The sequence from which to resend events.
   * @return The server session.
   */
  private ServerSession resendEvents(long version, long sequence) {
    if (version > eventAckVersion) {
      clearEvents(version, sequence);
      for (EventHolder holder : events) {
        sendEvent(holder);
      }
    }
    return this;
  }

  /**
   * Sends an event to the session.
   */
  private void sendEvent(EventHolder event) {
    if (connection != null) {
      connection.<PublishRequest, PublishResponse>send(PublishRequest.builder()
        .withSession(id())
        .withEventVersion(event.eventVersion)
        .withEventSequence(event.eventSequence)
        .withPreviousVersion(event.previousVersion)
        .withPreviousSequence(event.previousSequence)
        .withEvent(event.event)
        .withMessage(event.message)
        .build()).whenComplete((response, error) -> {
        if (isOpen() && error == null) {
          if (response.status() == Response.Status.OK) {
            clearEvents(response.version(), response.sequence());
          } else {
            clearEvents(response.version(), response.sequence());
            resendEvents(response.version(), response.sequence());
          }
        }
      });
    }
  }

  /**
   * Handles a publish request.
   *
   * @param request The publish request to handle.
   * @return A completable future to be completed with the publish response.
   */
  @SuppressWarnings("unchecked")
  protected CompletableFuture<PublishResponse> handlePublish(PublishRequest request) {
    Listeners<Object> listeners = eventListeners.get(request.event());
    if (listeners != null) {
      for (Listener listener : listeners) {
        listener.accept(request.message());
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
  void close() {
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
  void suspect() {
    suspect = true;
  }

  /**
   * Sets the session as trusted.
   */
  void trust() {
    suspect = false;
  }

  /**
   * Indicates whether the session is suspect.
   */
  boolean isSuspect() {
    return suspect;
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
   */
  void expire() {
    closed = true;
    expired = true;
    for (Listener<Session> listener : closeListeners) {
      listener.accept(this);
    }
  }

  @Override
  public boolean isExpired() {
    return expired;
  }

  @Override
  public String toString() {
    return String.format("Session[id=%d]", id);
  }

  /**
   * Event holder.
   */
  private static class EventHolder {
    private CompletableFuture<Void> future;
    private long eventVersion;
    private long eventSequence;
    private long previousVersion;
    private long previousSequence;
    private String event;
    private Object message;
  }

}
