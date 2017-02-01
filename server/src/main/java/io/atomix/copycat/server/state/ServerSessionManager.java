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
package io.atomix.copycat.server.state;

import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.server.session.Sessions;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Session manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ServerSessionManager implements Sessions {
  private final Map<Long, Connection> connections = new ConcurrentHashMap<>();
  final Map<Long, ServerSessionContext> sessions = new ConcurrentHashMap<>();
  final Set<SessionListener> listeners = new HashSet<>();
  private final ServerContext context;

  public ServerSessionManager(ServerContext context) {
    this.context = Assert.notNull(context, "context");
  }

  @Override
  public ServerSession session(long sessionId) {
    return sessions.get(sessionId);
  }

  @Override
  public Sessions addListener(SessionListener listener) {
    listeners.add(Assert.notNull(listener, "listener"));
    return this;
  }

  @Override
  public Sessions removeListener(SessionListener listener) {
    listeners.remove(Assert.notNull(listener, "listener"));
    return this;
  }

  /**
   * Registers a connection.
   */
  ServerSessionManager registerConnection(long sessionId, Connection connection) {
    ServerSessionContext session = sessions.get(sessionId);
    if (session != null) {
      session.setConnection(connection);
    }

    // It's possible for a connection to be registered before the RegisterEntry is applied on this
    // server, thus we have to store connection information even if a session doesn't exist.
    connections.put(sessionId, connection);
    return this;
  }

  /**
   * Unregisters a connection.
   */
  ServerSessionManager unregisterConnection(Connection connection) {
    Iterator<Map.Entry<Long, Connection>> iterator = connections.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, Connection> entry = iterator.next();
      if (entry.getValue().equals(connection)) {
        ServerSessionContext session = sessions.get(entry.getKey());
        if (session != null) {
          session.setConnection(null);
        }
        iterator.remove();
      }
    }
    return this;
  }

  /**
   * Registers a session.
   */
  ServerSessionContext registerSession(ServerSessionContext session) {
    session.setConnection(connections.get(session.id()));
    sessions.put(session.id(), session);
    return session;
  }

  /**
   * Unregisters a session.
   */
  ServerSessionContext unregisterSession(long sessionId) {
    ServerSessionContext session = sessions.remove(sessionId);
    if (session != null) {
      connections.remove(session.client(), session.getConnection());
    }
    return session;
  }

  /**
   * Gets a session by session ID.
   *
   * @param sessionId The session ID.
   * @return The session or {@code null} if the session doesn't exist.
   */
  ServerSessionContext getSession(long sessionId) {
    return sessions.get(sessionId);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<ServerSession> iterator() {
    return (Iterator) sessions.values().iterator();
  }

}
