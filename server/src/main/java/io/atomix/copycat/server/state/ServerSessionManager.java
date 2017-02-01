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

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.server.session.Sessions;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Session manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
class ServerSessionManager implements Sessions {
  private final Map<String, Address> addresses = new ConcurrentHashMap<>();
  private final Map<String, Connection> connections = new ConcurrentHashMap<>();
  final Map<Long, ServerSessionContext> sessions = new ConcurrentHashMap<>();
  final Map<String, ServerSessionContext> clients = new ConcurrentHashMap<>();
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
   * Registers an address.
   */
  ServerSessionManager registerAddress(String client, Address address) {
    ServerSessionContext session = clients.get(client);
    if (session != null) {
      session.setAddress(address);
      // If client was previously connected locally, close that connection.
      if (!address.equals(context.getCluster().member().serverAddress())) {
        Connection connection = connections.remove(client);
        if (connection != null) {
          connection.close();
          session.setConnection(null);
        }
      }
    }
    addresses.put(client, address);
    return this;
  }

  /**
   * Registers a connection.
   */
  ServerSessionManager registerConnection(String client, Connection connection) {
    ServerSessionContext session = clients.get(client);
    if (session != null) {
      session.setConnection(connection);
    }
    connections.put(client, connection);
    return this;
  }

  /**
   * Unregisters a connection.
   */
  ServerSessionManager unregisterConnection(Connection connection) {
    Iterator<Map.Entry<String, Connection>> iterator = connections.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Connection> entry = iterator.next();
      if (entry.getValue().equals(connection)) {
        ServerSessionContext session = clients.get(entry.getKey());
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
    session.setAddress(addresses.get(session.client()));
    session.setConnection(connections.get(session.client()));
    sessions.put(session.id(), session);
    clients.put(session.client(), session);
    return session;
  }

  /**
   * Unregisters a session.
   */
  ServerSessionContext unregisterSession(long sessionId) {
    ServerSessionContext session = sessions.remove(sessionId);
    if (session != null) {
      clients.remove(session.client(), session);
      addresses.remove(session.client(), session.getAddress());
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

  /**
   * Gets a session by client ID.
   *
   * @param clientId The client ID.
   * @return The session or {@code null} if the session doesn't exist.
   */
  ServerSessionContext getSession(String clientId) {
    return clients.get(clientId);
  }

  @Override
  @SuppressWarnings("unchecked")
  public Iterator<ServerSession> iterator() {
    return (Iterator) sessions.values().iterator();
  }

  public boolean hasConnection(long sessionId) {
    ServerSessionContext serverSessionContext = sessions.get(sessionId);
    Optional<Map.Entry<String, ServerSessionContext>> entry = clients.entrySet().stream().filter(k -> k.getValue().equals(serverSessionContext)).findFirst();
    if (entry.isPresent()) {
      String client = entry.get().getKey();
      return connections.containsKey(client);
    } else {
      // We don't know about this session, might be a newly registered. Just let it pass for now.
      return true;
    }
  }
}
