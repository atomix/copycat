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

import io.atomix.catalyst.concurrent.Listener;

import java.util.function.Consumer;

/**
 * Represents a client's connection to the Copycat cluster.
 * <p>
 * Each client that connects to a Raft cluster must open a {@link Session} in order to submit operations to the cluster.
 * When a client first connects to a server, it must register a new session. Once the session has been registered,
 * it can be used to submit {@link io.atomix.copycat.Command commands} and {@link io.atomix.copycat.Query queries}
 * to the cluster.
 * <p>
 * Sessions represent a connection between a single client and all servers in a Raft cluster. Session information
 * is replicated via the Raft consensus algorithm, and clients can safely switch connections between servers without
 * losing their session. All consistency guarantees are provided within the context of a session. Once a session is
 * expired or closed, linearizability, sequential consistency, and other guarantees for events and operations are
 * effectively lost.
 * <p>
 * Throughout the lifetime of a session it can transition between a number of different
 * {@link io.atomix.copycat.session.Session.State states}. Session states are indicative of the client's ability to
 * maintain its connections to the cluster and the cluster's ability to provide certain semantics within the context
 * of the session. The current state of a session can be read via the {@link #state()} method, and users can react to
 * changes in the session state by registering a state change listener via {@link #onStateChange(Consumer)}.
 * <p>
 * The ideal state of a session is the {@link Session.State#OPEN OPEN} state which indicates that the client has
 * registered a session and has successfully submitted a keep-alive request within the last session timeout. In the
 * event that a client cannot communicate with any server in the cluster or the cluster has not received a keep-alive
 * from a client within a session timeout, the session's state will be changed to {@link Session.State#UNSTABLE UNSTABLE},
 * indicating that the session may be expired. While in the {@code UNSTABLE} state, Copycat cannot guarantee linearizable
 * semantics for operations. From the {@code UNSTABLE} state, the session will either transition back to the
 * {@code OPEN} state or into the {@link Session.State#EXPIRED EXPIRED} state, indicating that linearizability guarantees
 * have been lost. Finally, sessions that are explicitly unregistered by a client will be transitioned to the
 * {@link Session.State#CLOSED CLOSED} state.
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
     * Indicates that the session has been in unstable state for sometime and may or may not be {@link #EXPIRED}.
     * <p>
     * The stale state has same semantics as that of unstable state.
     * </p>
     */
    STALE(true),
    
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
