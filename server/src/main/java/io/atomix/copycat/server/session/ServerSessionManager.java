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
package io.atomix.copycat.server.session;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.atomix.copycat.client.session.Session;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Session manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ServerSessionManager implements Sessions {
  private final Map<Long, Address> addresses = new ConcurrentHashMap<>();
  private final Map<Long, Connection> connections = new ConcurrentHashMap<>();
  final Map<Long, ServerSession> sessions = new ConcurrentHashMap<>();

  @Override
  public Session session(long sessionId) {
    return sessions.get(sessionId);
  }

  /**
   * Registers an address.
   */
  public ServerSessionManager registerAddress(long sessionId, Address address) {
    ServerSession session = sessions.get(sessionId);
    if (session != null) {
      session.setAddress(address);
    }
    addresses.put(sessionId, address);
    return this;
  }

  /**
   * Registers a connection.
   */
  public ServerSessionManager registerConnection(long sessionId, Connection connection) {
    ServerSession session = sessions.get(sessionId);
    if (session != null) {
      session.setConnection(connection);
    }
    connections.put(sessionId, connection);
    return this;
  }

  /**
   * Unregisters a connection.
   */
  public ServerSessionManager unregisterConnection(Connection connection) {
    Iterator<Map.Entry<Long, Connection>> iterator = connections.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<Long, Connection> entry = iterator.next();
      if (entry.getValue().equals(connection)) {
        ServerSession session = sessions.get(entry.getKey());
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
  public ServerSession registerSession(ServerSession session) {
    session.setAddress(addresses.get(session.id()));
    session.setConnection(connections.get(session.id()));
    sessions.put(session.id(), session);
    return session;
  }

  /**
   * Unregisters a session.
   */
  public ServerSession unregisterSession(long sessionId) {
    addresses.remove(sessionId);
    connections.remove(sessionId);
    return sessions.remove(sessionId);
  }

  /**
   * Gets a session by session ID.
   *
   * @param sessionId The session ID.
   * @return The session or {@code null} if the session doesn't exist.
   */
  public ServerSession getSession(long sessionId) {
    return sessions.get(sessionId);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<Session> iterator() {
    return (Iterator) sessions.values().iterator();
  }

}
