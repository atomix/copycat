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

import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.server.session.Sessions;

import java.util.*;

/**
 * State machine sessions.
 */
class ServerStateMachineSessions implements Sessions {
  final Map<Long, ServerSessionContext> sessions = new HashMap<>();
  final Set<SessionListener> listeners = new HashSet<>();

  /**
   * Adds a session to the sessions list.
   *
   * @param session The session to add.
   */
  void add(ServerSessionContext session) {
    sessions.put(session.id(), session);
  }

  /**
   * Removes a session from the sessions list.
   *
   * @param session The session to remove.
   */
  void remove(ServerSessionContext session) {
    sessions.remove(session.id());
  }

  @Override
  public ServerSession session(long sessionId) {
    return sessions.get(sessionId);
  }

  @Override
  public Sessions addListener(SessionListener listener) {
    listeners.add(listener);
    return this;
  }

  @Override
  public Sessions removeListener(SessionListener listener) {
    listeners.remove(listener);
    return this;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<ServerSession> iterator() {
    return (Iterator) sessions.values().iterator();
  }
}