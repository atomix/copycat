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
package io.atomix.copycat.client.impl;

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.CopycatClient;

import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.Consumer;

/**
 * Client state.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class CopycatClientState {
  private volatile long id;
  private final String uuid;
  private volatile CopycatClient.State state = CopycatClient.State.CLOSED;
  private final Set<Listener<CopycatClient.State>> changeListeners = new CopyOnWriteArraySet<>();

  CopycatClientState(String uuid) {
    this.uuid = Assert.notNull(uuid, "uuid");
  }

  /**
   * Returns the client ID.
   *
   * @return The client ID.
   */
  public long getId() {
    return id;
  }

  /**
   * Sets the client ID.
   *
   * @param id The client ID.
   * @return The client state.
   */
  public CopycatClientState setId(long id) {
    this.id = id;
    return this;
  }

  /**
   * Returns the client's UUID.
   *
   * @return The client's UUID.
   */
  public String getUuid() {
    return uuid;
  }

  /**
   * Returns the session state.
   *
   * @return The session state.
   */
  public CopycatClient.State getState() {
    return state;
  }

  /**
   * Sets the session state.
   *
   * @param state The session state.
   * @return The session state.
   */
  public CopycatClientState setState(CopycatClient.State state) {
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
  public Listener<CopycatClient.State> onStateChange(Consumer<CopycatClient.State> callback) {
    Listener<CopycatClient.State> listener = new Listener<CopycatClient.State>() {
      @Override
      public void accept(CopycatClient.State state) {
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

}
