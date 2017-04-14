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

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Client state.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
final class ClientSessionState {
  private static final Logger LOGGER = LoggerFactory.getLogger(ClientSession.class);
  private final String clientId;
  private volatile long sessionId;
  private volatile Session.State state = Session.State.CLOSED;
  private long commandRequest;
  private long commandResponse;
  private long responseIndex;
  private long eventIndex;
  private long unstableSince;
  private long unstabilityTimeout;
  private final Set<Listener<Session.State>> changeListeners = new CopyOnWriteArraySet<>();

  ClientSessionState(String clientId) {
    this(clientId, Duration.ZERO);
  }

  ClientSessionState(String clientId, Duration unstabilityTimeout) {
    this.clientId = Assert.notNull(clientId, "clientId");
    this.unstabilityTimeout = Assert.notNull(unstabilityTimeout, "unstabilityTimeout").toMillis();
  }

  /**
   * Returns the client ID.
   *
   * @return The client ID.
   */
  public String getClientId() {
    return clientId;
  }

  /**
   * Returns the session logger.
   *
   * @return The session logger.
   */
  public Logger getLogger() {
    return LOGGER;
  }

  /**
   * Sets the client session ID.
   *
   * @param sessionId The client session ID.
   * @return The client session state.
   */
  public ClientSessionState setSessionId(long sessionId) {
    this.sessionId = sessionId;
    this.responseIndex = sessionId;
    this.eventIndex = sessionId;
    return this;
  }

  /**
   * Returns the client session ID.
   *
   * @return The client session ID.
   */
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
    if (state != Session.State.UNSTABLE) {
      if (this.state != state) {
        return setStateAndCallListeners(state);
      }
    } else {
      if (this.state == Session.State.UNSTABLE) {
        if (unstabilityTimeout > 0 && (System.currentTimeMillis() - unstableSince) > unstabilityTimeout) {
          return setStateAndCallListeners(Session.State.STALE);
        }
      } else if (this.state != Session.State.STALE) {
        unstableSince = System.currentTimeMillis();
        return setStateAndCallListeners(state);
      }
    }

    return this;
  }

  private ClientSessionState setStateAndCallListeners(Session.State state) {
    this.state = state;
    changeListeners.forEach(l -> l.accept(state));
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

  /**
   * Sets the last command request sequence number.
   *
   * @param commandRequest The last command request sequence number.
   * @return The client session state.
   */
  public ClientSessionState setCommandRequest(long commandRequest) {
    this.commandRequest = commandRequest;
    return this;
  }

  /**
   * Returns the last command request sequence number for the session.
   *
   * @return The last command request sequence number for the session.
   */
  public long getCommandRequest() {
    return commandRequest;
  }

  /**
   * Returns the next command request sequence number for the session.
   *
   * @return The next command request sequence number for the session.
   */
  public long nextCommandRequest() {
    return ++commandRequest;
  }

  /**
   * Sets the last command sequence number for which a response has been received.
   *
   * @param commandResponse The last command sequence number for which a response has been received.
   * @return The client session state.
   */
  public ClientSessionState setCommandResponse(long commandResponse) {
    this.commandResponse = commandResponse;
    return this;
  }

  /**
   * Returns the last command sequence number for which a response has been received.
   *
   * @return The last command sequence number for which a response has been received.
   */
  public long getCommandResponse() {
    return commandResponse;
  }

  /**
   * Sets the highest index for which a response has been received.
   *
   * @param responseIndex The highest index for which a command or query response has been received.
   * @return The client session state.
   */
  public ClientSessionState setResponseIndex(long responseIndex) {
    this.responseIndex = Math.max(this.responseIndex, responseIndex);
    return this;
  }

  /**
   * Returns the highest index for which a response has been received.
   *
   * @return The highest index for which a command or query response has been received.
   */
  public long getResponseIndex() {
    return responseIndex;
  }

  /**
   * Sets the highest index for which an event has been received in sequence.
   *
   * @param eventIndex The highest index for which an event has been received in sequence.
   * @return The client session state.
   */
  public ClientSessionState setEventIndex(long eventIndex) {
    this.eventIndex = eventIndex;
    return this;
  }

  /**
   * Returns the highest index for which an event has been received in sequence.
   *
   * @return The highest index for which an event has been received in sequence.
   */
  public long getEventIndex() {
    return eventIndex;
  }

}
