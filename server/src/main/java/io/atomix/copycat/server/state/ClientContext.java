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
package io.atomix.copycat.server.state;

import io.atomix.copycat.server.storage.LogCleaner;

/**
 * Client context.
 */
public class ClientContext {
  private final long id;
  private final long timeout;
  private final LogCleaner cleaner;
  private State state = State.OPEN;
  private long timestamp;
  private long keepAliveIndex;
  private volatile boolean unregistering = false;

  public ClientContext(long id, long timeout, LogCleaner cleaner) {
    this.id = id;
    this.timeout = timeout;
    this.cleaner = cleaner;
  }

  /**
   * Returns the client ID.
   *
   * @return The client ID.
   */
  public long id() {
    return id;
  }

  /**
   * Returns the client timeout.
   *
   * @return The client timeout.
   */
  public long timeout() {
    return timeout;
  }

  /**
   * Returns the client state.
   *
   * @return The client state.
   */
  public State state() {
    return state;
  }

  /**
   * Updates the client state.
   *
   * @param state The updated client state.
   */
  private void setState(State state) {
    if (this.state != state) {
      this.state = state;
    }
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
   */
  private void updateTimestamp(long timestamp) {
    this.timestamp = Math.max(this.timestamp, timestamp);
  }

  /**
   * Registers the client's session.
   *
   * @param timestamp The timestamp of the register.
   */
  void open(long timestamp) {
    // Update the client's timestamp.
    updateTimestamp(timestamp);

    // Set the client's state to OPEN.
    setState(State.OPEN);
  }

  /**
   * Keeps the client's session alive.
   *
   * @param index The index of the keep-alive.
   * @param timestamp The keep-alive timestamp.
   */
  void keepAlive(long index, long timestamp) {
    // Update the client's timestamp.
    updateTimestamp(timestamp);

    // Set the client's state to OPEN.
    setState(State.OPEN);

    // If an old keep-alive was applied, clean it from the log.
    long oldKeepAliveIndex = this.keepAliveIndex;
    if (oldKeepAliveIndex > 0) {
      cleaner.clean(oldKeepAliveIndex);
    }
    this.keepAliveIndex = index;
  }

  /**
   * Sets the session as suspect.
   *
   * @param timestamp The suspicion timestamp.
   */
  void suspect(long timestamp) {
    // Update the client's timestamp.
    updateTimestamp(timestamp);

    // Set the client's state to SUSPICIOUS.
    setState(State.SUSPICIOUS);
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
   * Closes the session.
   *
   * @param index The index at which to close the session.
   */
  void close(long index) {
    // Set the client's state to CLOSED.
    setState(State.CLOSED);

    // Release the client's RegisterEntry from the log.
    cleaner.clean(id);

    // Release the UnregisterEntry from the log.
    cleaner.clean(index);
  }

  /**
   * Client state.
   */
  public enum State {
    OPEN,
    SUSPICIOUS,
    CLOSED,
  }

}
