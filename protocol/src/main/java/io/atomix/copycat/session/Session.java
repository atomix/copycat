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
package io.atomix.copycat.session;

import io.atomix.catalyst.util.Listener;

import java.util.function.Consumer;

/**
 * Provides event-based methods for monitoring Raft sessions and communicating between Raft clients and servers.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Session {

  /**
   * Represents the state of a session.
   * <p>
   * Throughout the lifetime of a session, the session may change state as a result of communication or the lack
   * thereof between the client and the cluster. See the specific states for more documentation.
   */
  enum State {

    /**
     * Indicates that the session is connected and open.
     * <p>
     * This is the initial state of a session upon registration. Once the session is registered with the cluster,
     * the session's state will be {@code OPEN} and will remain open so long as the client is able to communicate
     * with the cluster to keep its session alive.
     * <p>
     * Clients withe a session in the {@code OPEN} state can be assumed to be operating normally and are guaranteed
     * to benefit from linearizable reads and writes.
     */
    OPEN(true),

    /**
     * Indicates that the session in an unstable state and may or may not be {@link #EXPIRED}.
     * <p>
     * The unstable state is indicative of a state in which a client is unable to communicate with the cluster and
     * therefore cannot determine whether its session is or is not expired. Until the client is able to re-establish
     * communication with the cluster, its session will remain in this state. While in the {@code UNSTABLE} state,
     * users of the client should assume that the session may have been expired and that other clients may react
     * to the client having been expired while in the unstable state.
     * <p>
     * Commands submitted or completed in the {@code UNSTABLE} state may not be linearizable. An unstable session
     * may be expired and a client may have to resubmit associated commands once a new session in registered. This
     * can lead to duplicate commands being applied to servers state machines, thus breaking linearizability.
     * <p>
     * Once the client is able to re-establish communication with the cluster again it will determine whether the
     * session is still active or indeed has been expired. The session will either transition back to the
     * {@link #OPEN} state or {@link #EXPIRED} based on feedback from the cluster. Only the cluster can explicitly
     * expire a session.
     */
    UNSTABLE(true),

    /**
     * Indicates that the session is expired.
     * <p>
     * Once an {@link #UNSTABLE} client re-establishes communication with the cluster, the cluster may indicate to
     * the client that its session has been expired. In that case, the client may no longer submit operations under
     * the expired session and must register a new session to continue operating on server state machines.
     * <p>
     * When a client's session is expired, commands submitted under the expired session that have not yet been completed
     * may or may not be applied to server state machines. Linearizability is guaranteed only within the context of a
     * session, and so linearizability may be broken if operations are resubmitted across sessions.
     */
    EXPIRED(false),

    /**
     * Indicates that the session has been closed.
     * <p>
     * This state indicates that the client's session was explicitly unregistered and the session was closed safely.
     */
    CLOSED(false);

    private final boolean active;

    State(boolean active) {
      this.active = active;
    }

    /**
     * Returns a boolean value indicating whether the state is an active state.
     * <p>
     * Sessions can only submit commands and receive events while in an active state.
     *
     * @return Indicates whether the state is an active state.
     */
    public boolean active() {
      return active;
    }
  }

  /**
   * Returns the session ID.
   * <p>
   * The session ID is unique to an individual session within the cluster. That is, it is guaranteed that
   * no two clients will have a session with the same ID.
   *
   * @return The session ID.
   */
  long id();

  /**
   * Returns the current session state.
   *
   * @return The current session state.
   */
  State state();

  /**
   * Registers a callback to be called when the session state changes.
   *
   * @param callback The callback to be called when the session state changes.
   * @return The state change listener.
   */
  Listener<State> onStateChange(Consumer<State> callback);

}
