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

import io.atomix.copycat.error.UnknownSessionException;
import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.atomix.copycat.protocol.request.PublishRequest;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.response.PublishResponse;
import io.atomix.copycat.session.Event;
import io.atomix.copycat.util.Assert;
import io.atomix.copycat.util.concurrent.Futures;
import io.atomix.copycat.util.concurrent.Listener;
import io.atomix.copycat.util.concurrent.ThreadContext;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Client session message listener.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class ClientSessionListener {
  private final ClientSessionState state;
  private final ThreadContext context;
  private final Map<String, Set<Consumer>> eventListeners = new ConcurrentHashMap<>();
  private final ClientSequencer sequencer;

  public ClientSessionListener(ProtocolClientConnection connection, ClientSessionState state, ClientSequencer sequencer, ThreadContext context) {
    this.state = Assert.notNull(state, "state");
    this.context = Assert.notNull(context, "context");
    this.sequencer = Assert.notNull(sequencer, "sequencer");
    connection.onPublish(this::handlePublish);
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
    Set<Consumer> listeners = eventListeners.computeIfAbsent(event, e -> new CopyOnWriteArraySet<>());
    listeners.add(listener);
    return new Listener<T>() {
      @Override
      public void accept(T event) {
        listener.accept(event);
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
  private CompletableFuture<PublishResponse> handlePublish(PublishRequest request, PublishResponse.Builder builder) {
    state.getLogger().debug("{} - Received {}", state.getSessionId(), request);

    // If the request is for another session ID, this may be a session that was previously opened
    // for this client.
    if (request.session() != state.getSessionId()) {
      state.getLogger().debug("{} - Inconsistent session ID: {}", state.getSessionId(), request.session());
      return Futures.exceptionalFuture(new UnknownSessionException("incorrect session ID"));
    }

    if (request.eventIndex() <= state.getEventIndex()) {
      return CompletableFuture.completedFuture(
        builder.withStatus(ProtocolResponse.Status.OK)
          .withIndex(state.getEventIndex())
          .build());
    }

    // If the request's previous event index doesn't equal the previous received event index,
    // respond with an undefined error and the last index received. This will cause the cluster
    // to resend events starting at eventIndex + 1.
    if (request.previousIndex() != state.getEventIndex()) {
      state.getLogger().debug("{} - Inconsistent event index: {}", state.getSessionId(), request.previousIndex());
      return CompletableFuture.completedFuture(
        builder.withStatus(ProtocolResponse.Status.ERROR)
          .withIndex(state.getEventIndex())
          .build());
    }

    // Store the event index. This will be used to verify that events are received in sequential order.
    state.setEventIndex(request.eventIndex());

    sequencer.sequenceEvent(request, () -> {
      for (Event<?> event : request.events()) {
        Set<Consumer> listeners = eventListeners.get(event.name());
        if (listeners != null) {
          for (Consumer listener : listeners) {
            listener.accept(event.message());
          }
        }
      }
    });

    return CompletableFuture.completedFuture(
      builder.withStatus(ProtocolResponse.Status.OK)
        .withIndex(request.eventIndex())
        .build());
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
