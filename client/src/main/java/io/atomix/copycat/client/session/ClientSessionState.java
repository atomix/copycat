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

import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.Listener;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Client state.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class ClientSessionState {
  private final UUID clientId;
  private volatile long sessionId;
  private Session.State state = Session.State.CLOSED;
  private long commandRequest;
  private long commandResponse;
  private long requestSequence;
  private long responseSequence;
  private long responseIndex;
  private long eventIndex;
  private long completeIndex;
  private final Set<Listener<Session.State>> changeListeners = new CopyOnWriteArraySet<>();

  ClientSessionState(UUID clientId) {
    this.clientId = Assert.notNull(clientId, "clientId");
  }

  public UUID getClientId() {
    return clientId;
  }

  public ClientSessionState setSessionId(long sessionId) {
    this.sessionId = sessionId;
    return this;
  }

  public long getSessionId() {
    return sessionId;
  }

  /**
   * Returns the session state.
   *
   * @return The session state.
   */
  public Session.State getState() {
    return state;
  }

  /**
   * Sets the session state.
   *
   * @param state The session state.
   * @return The session state.
   */
  public ClientSessionState setState(Session.State state) {
    if (this.state != state) {
      this.state = state;
      changeListeners.forEach(l -> l.accept(state));
    }
    return this;
  }

  /**
   * Registers a state change listener on the session manager.
   *
   * @param callback The state change listener callback.
   * @return The state change listener.
   */
  public Listener<Session.State> onStateChange(Consumer<Session.State> callback) {
    Listener<Session.State> listener = new Listener<Session.State>() {
      @Override
      public void accept(Session.State state) {
        callback.accept(state);
      }
      @Override
      public void close() {
        changeListeners.remove(this);
      }
    };
    changeListeners.add(listener);
    return listener;
  }

  public ClientSessionState setCommandRequest(long commandRequest) {
    this.commandRequest = commandRequest;
    return this;
  }

  public long getCommandRequest() {
    return commandRequest;
  }

  public long nextCommandRequest() {
    return ++commandRequest;
  }

  public ClientSessionState setCommandResponse(long commandResponse) {
    this.commandResponse = commandResponse;
    return this;
  }

  public long getCommandResponse() {
    return commandResponse;
  }

  public ClientSessionState setRequestSequence(long requestSequence) {
    this.requestSequence = requestSequence;
    return this;
  }

  public long getRequestSequence() {
    return requestSequence;
  }

  public long nextRequestSequence() {
    return ++requestSequence;
  }

  public ClientSessionState setResponseSequence(long responseSequence) {
    this.responseSequence = responseSequence;
    return this;
  }

  public long getResponseSequence() {
    return responseSequence;
  }

  public ClientSessionState setResponseIndex(long responseIndex) {
    this.responseIndex = Math.max(this.responseIndex, responseIndex);
    return this;
  }

  public long getResponseIndex() {
    return responseIndex;
  }

  public ClientSessionState setEventIndex(long eventIndex) {
    this.eventIndex = eventIndex;
    return this;
  }

  public long getEventIndex() {
    return eventIndex;
  }

  public ClientSessionState setCompleteIndex(long completeIndex) {
    this.completeIndex = Math.max(this.completeIndex, completeIndex);
    return this;
  }

  public long getCompleteIndex() {
    return completeIndex;
  }

}
