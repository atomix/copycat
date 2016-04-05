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
package io.atomix.copycat.client.session;

import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.Event;
import io.atomix.copycat.client.util.ClientSequencer;
import io.atomix.copycat.error.UnknownSessionException;
import io.atomix.copycat.protocol.PublishRequest;
import io.atomix.copycat.protocol.PublishResponse;
import io.atomix.copycat.protocol.Response;
import io.atomix.copycat.session.SessionEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Client session message listener.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class ClientSessionListener {
  private final ClientSessionState state;
  private final ThreadContext context;
  private final Map<String, List<Consumer<Event>>> eventListeners = new ConcurrentHashMap<>();
  private final ClientSequencer sequencer = new ClientSequencer();

  public ClientSessionListener(Connection connection, ClientSessionState state, ThreadContext context) {
    this.state = Assert.notNull(state, "state");
    this.context = Assert.notNull(context, "context");
    connection.handler(PublishRequest.class, this::handlePublish);
  }

  /**
   * Registers a session event listener.
   */
  @SuppressWarnings("unchecked")
  public synchronized <T> Listener<Event<T>> onEvent(String event, Consumer<Event<T>> listener) {
    List<Consumer<Event>> listeners = eventListeners.computeIfAbsent(Assert.notNull(event, "event"), e -> new ArrayList<>());
    listeners.add((Consumer) listener);
    return new Listener<Event<T>>() {
      @Override
      public void accept(Event<T> value) {
        listener.accept(value);
      }
      @Override
      public void close() {
        listeners.remove(listener);
      }
    };
  }

  /**
   * Handles a publish request.
   *
   * @param request The publish request to handle.
   * @return A completable future to be completed with the publish response.
   */
  @SuppressWarnings("unchecked")
  private CompletableFuture<PublishResponse> handlePublish(PublishRequest request) {
    state.getLogger().debug("{} - Received {}", state.getSessionId(), request);

    // If the request is for another session ID, this may be a session that was previously opened
    // for this client.
    if (request.session() != state.getSessionId()) {
      state.getLogger().debug("{} - Inconsistent session ID: {}", state.getSessionId(), request.session());
      return Futures.exceptionalFuture(new UnknownSessionException("incorrect session ID"));
    }

    // If the request's previous event index doesn't equal the previous received event index,
    // respond with an undefined error and the last index received. This will cause the cluster
    // to resend events starting at eventIndex + 1.
    if (request.previousIndex() != state.getEventIndex()) {
      state.getLogger().debug("{} - Inconsistent event index: {}", state.getSessionId(), request.previousIndex());
      return CompletableFuture.completedFuture(PublishResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withEventIndex(state.getEventIndex())
        .withCompleteIndex(state.getCompleteIndex())
        .build());
    }

    // Store the event index. This will be used to verify that events are received in sequential order.
    state.setEventIndex(request.eventIndex());

    CompletableFuture<PublishResponse> future = new CompletableFuture<>();

    // Store a sequence number to sequence event completions.
    long sequenceNumber = sequencer.nextSequence();

    // Create a counter to track the total number of event listeners invoked and the ack count.
    AtomicInteger completionCount = new AtomicInteger();
    AtomicInteger totalCount = new AtomicInteger();

    // When an event is completed, the completion count is incremented until the total count for
    // the publish request has been reached. Once all events have been completed, the client's
    // complete index is updated and the response is sent back to the server.
    Runnable completionCallback = () -> {
      if (completionCount.incrementAndGet() == totalCount.get()) {
        sequencer.sequence(sequenceNumber, () -> {
          state.setCompleteIndex(request.eventIndex());
          future.complete(PublishResponse.builder()
            .withStatus(Response.Status.OK)
            .withEventIndex(state.getEventIndex())
            .withCompleteIndex(state.getCompleteIndex())
            .build());
        });
      }
    };

    // For each event in the request, trigger all relevant event listeners. For each listener, create
    // a separate local event object that can only be completed once.
    for (Event<?> event : request.events()) {
      List<Consumer<Event>> listeners = eventListeners.get(event.name());
      if (listeners != null) {
        for (Consumer<Event> listener : listeners) {
          totalCount.incrementAndGet();
          context.executor().execute(() -> listener.accept(new SessionEvent<>(event.name(), event.message(), completionCallback)));
        }
      }
    }
    return future;
  }

  /**
   * Closes the session event listener.
   *
   * @return A completable future to be completed once the listener is closed.
   */
  public CompletableFuture<Void> close() {
    return CompletableFuture.completedFuture(null);
  }

}
