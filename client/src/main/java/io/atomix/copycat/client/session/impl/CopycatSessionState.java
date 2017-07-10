/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.copycat.client.session.impl;

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.copycat.client.session.CopycatSession;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Client state.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class CopycatSessionState {
  private final long sessionId;
  private final String name;
  private final String type;
  private final AtomicBoolean open = new AtomicBoolean(true);
  private long commandRequest;
  private long commandResponse;
  private long responseIndex;
  private long eventIndex;
  private long connection;
  private final Set<Listener<CopycatSession.State>> changeListeners = new CopyOnWriteArraySet<>();

  CopycatSessionState(long sessionId, String name, String type) {
    this.sessionId = sessionId;
    this.name = name;
    this.type = type;
    this.responseIndex = sessionId;
    this.eventIndex = sessionId;
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
   * Returns the session name.
   *
   * @return The session name.
   */
  public String getSessionName() {
    return name;
  }

  /**
   * Returns the session type.
   *
   * @return The session type.
   */
  public String getSessionType() {
    return type;
  }

  /**
   * Returns the session state.
   *
   * @return The session state.
   */
  public CopycatSession.State getState() {
    return isOpen() ? CopycatSession.State.OPEN : CopycatSession.State.CLOSED;
  }

  /**
   * Returns a boolean indicating whether the session is open.
   *
   * @return Whether the session is open.
   */
  public boolean isOpen() {
    return open.get();
  }

  /**
   * Closes the session.
   */
  void close() {
    if (open.compareAndSet(true, false)) {
      changeListeners.forEach(l -> l.accept(CopycatSession.State.CLOSED));
    }
  }

  /**
   * Registers a state change listener on the session manager.
   *
   * @param callback The state change listener callback.
   * @return The state change listener.
   */
  public Listener<CopycatSession.State> onStateChange(Consumer<CopycatSession.State> callback) {
    Listener<CopycatSession.State> listener = new Listener<CopycatSession.State>() {
      @Override
      public void accept(CopycatSession.State state) {
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
  public CopycatSessionState setCommandRequest(long commandRequest) {
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
  public CopycatSessionState setCommandResponse(long commandResponse) {
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
  public CopycatSessionState setResponseIndex(long responseIndex) {
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
  public CopycatSessionState setEventIndex(long eventIndex) {
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

  /**
   * Sets the session's current connection.
   *
   * @param connection The session's current connection.
   * @return The client session state.
   */
  public CopycatSessionState setConnection(int connection) {
    this.connection = connection;
    return this;
  }

  /**
   * Returns the session's current connection.
   *
   * @return The session's current connection.
   */
  public long getConnection() {
    return connection;
  }

  /**
   * Returns the session's next connection ID.
   *
   * @return The session's next connection ID.
   */
  public long nextConnection() {
    return ++connection;
  }

}
