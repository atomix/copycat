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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Session manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ServerSessionManager {
  private final Map<Long, ServerSessionContext> sessions = new ConcurrentHashMap<>();
  private final Map<Long, TimestampedConnection> connections = new ConcurrentHashMap<>();

  /**
   * Registers a connection.
   */
  void registerConnection(long sessionId, long connectionId, Connection connection) {
    ServerSessionContext session = sessions.get(sessionId);
    TimestampedConnection existingConnection = connections.get(sessionId);
    if (existingConnection == null || existingConnection.id < connectionId) {
      connections.put(sessionId, new TimestampedConnection(connectionId, connection));
      if (session != null) {
        session.setConnection(connection);
      }
    }
  }

  /**
   * Registers a connection.
   */
  void registerConnection(long sessionId, long connectionId) {
    TimestampedConnection connection = connections.get(sessionId);
    if (connection != null && connection.id < connectionId) {
      connections.remove(sessionId, connection);
    }
  }

  /**
   * Unregisters a connection.
   */
  void unregisterConnection(Connection connection) {
    Iterator<Map.Entry<Long, TimestampedConnection>> iterator = connections.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, TimestampedConnection> entry = iterator.next();
      if (entry.getValue().connection.equals(connection)) {
        ServerSessionContext session = sessions.get(entry.getKey());
        if (session != null) {
          session.setConnection(null);
        }
        iterator.remove();
      }
    }
  }

  /**
   * Registers a session.
   */
  void registerSession(ServerSessionContext session) {
    sessions.put(session.id(), session);
    TimestampedConnection connection = connections.get(session.id());
    if (connection != null) {
      session.setConnection(connection.connection);
    }
  }

  /**
   * Unregisters a session.
   */
  void unregisterSession(long sessionId) {
    sessions.remove(sessionId);
    connections.remove(sessionId);
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

  /**
   * Returns the collection of registered sessions.
   *
   * @return The collection of registered sessions.
   */
  Collection<ServerSessionContext> getSessions() {
    return sessions.values();
  }

  /**
   * Connection ID/connection holder.
   */
  private static class TimestampedConnection {
    private final long id;
    private final Connection connection;

    TimestampedConnection(long id, Connection connection) {
      this.id = id;
      this.connection = connection;
    }
  }

}
