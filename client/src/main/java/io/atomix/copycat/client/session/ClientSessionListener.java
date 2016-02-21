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
import io.atomix.catalyst.util.Listeners;
import io.atomix.catalyst.util.concurrent.Futures;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.error.UnknownSessionException;
import io.atomix.copycat.protocol.PublishRequest;
import io.atomix.copycat.protocol.PublishResponse;
import io.atomix.copycat.protocol.Response;
import io.atomix.copycat.session.Event;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Client session message listener.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class ClientSessionListener {
  private final ClientSessionState state;
  private final ThreadContext context;
  private final Map<String, Listeners<Object>> eventListeners = new ConcurrentHashMap<>();

  public ClientSessionListener(Connection connection, ClientSessionState state, ThreadContext context) {
    this.state = Assert.notNull(state, "state");
    this.context = Assert.notNull(context, "context");
    connection.handler(PublishRequest.class, this::handlePublish);
  }

  /**
   * Registers a session event listener.
   */
  @SuppressWarnings("unchecked")
  public Listener<Void> onEvent(String event, Runnable callback) {
    return onEvent(event, v -> callback.run());
  }

  /**
   * Registers a session event listener.
   */
  @SuppressWarnings("unchecked")
  public <T> Listener<T> onEvent(String event, Consumer listener) {
    return (Listener<T>) eventListeners.computeIfAbsent(Assert.notNull(event, "event"), e -> new Listeners<>())
      .add(Assert.notNull(listener, "listener"));
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
        .withIndex(state.getEventIndex())
        .build());
    }

    // Store the event index. This will be used to verify that events are received in sequential order.
    state.setEventIndex(request.eventIndex());

    // For each event in the events batch, call the appropriate event listener and create a CompletableFuture
    // to be called once the event callback is complete. Futures will ensure that an event is not acknowledged
    // until all event callbacks have completed.
    List<CompletableFuture<Void>> futures = new ArrayList<>(request.events().size());
    for (Event<?> event : request.events()) {
      Listeners<Object> listeners = eventListeners.get(event.name());
      if (listeners != null) {
        futures.add(listeners.accept(event.message()));
      }
    }

    // Wait for all event listeners to complete and then respond to the event message. This ensures that
    // linearizable events are completed between their invocation and response. If the async queue is backed
    // up and we don't wait for callbacks to complete, the cluster will believe an event to have been received
    // and handled before it has indeed been received and handled.
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()]))
      .handleAsync((result, error) -> {
        // Store the highest index for which event callbacks have completed.
        state.setCompleteIndex(request.eventIndex());

        return PublishResponse.builder()
          .withStatus(Response.Status.OK)
          .withIndex(state.getCompleteIndex())
          .build();
      }, context.executor());
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
