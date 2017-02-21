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

import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.server.protocol.RaftProtocolClient;
import io.atomix.copycat.server.protocol.RaftProtocolClientConnection;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Connection manager.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public final class ConnectionManager {
  private final RaftProtocolClient client;
  private final Map<Address, RaftProtocolClientConnection> connections = new HashMap<>();
  private final Map<Address, CompletableFuture<RaftProtocolClientConnection>> futures = new HashMap<>();

  public ConnectionManager(RaftProtocolClient client) {
    this.client = client;
  }

  /**
   * Returns the connection for the given member.
   *
   * @param address The member for which to get the connection.
   * @return A completable future to be called once the connection is received.
   */
  public synchronized CompletableFuture<RaftProtocolClientConnection> getConnection(Address address) {
    RaftProtocolClientConnection connection = connections.get(address);
    if (connection == null) {
      CompletableFuture<RaftProtocolClientConnection> future = futures.get(address);
      if (future == null) {
        future = createConnection(address);
        future.whenComplete((c, e) -> futures.remove(address));
        futures.put(address, future);
      }
      return future;
    }
    return CompletableFuture.completedFuture(connection);
  }

  /**
   * Resets the connection to the given address.
   *
   * @param address The address for which to reset the connection.
   */
  public synchronized void resetConnection(Address address) {
    RaftProtocolClientConnection connection = connections.remove(address);
    if (connection != null) {
      connection.close();
    }
  }

  /**
   * Creates a connection for the given member.
   *
   * @param address The member for which to create the connection.
   * @return A completable future to be called once the connection has been created.
   */
  private synchronized CompletableFuture<RaftProtocolClientConnection> createConnection(Address address) {
    return client.connect(address).thenApply(connection -> {
      connection.onClose(c -> {
        if (connections.get(address) == c) {
          connections.remove(address);
        }
      });
      connections.put(address, connection);
      return connection;
    });
  }

  /**
   * Closes the connection manager.
   *
   * @return A completable future to be completed once the connection manager is closed.
   */
  public synchronized CompletableFuture<Void> close() {
    CompletableFuture[] futures = new CompletableFuture[connections.size()];
    int i = 0;
    for (RaftProtocolClientConnection connection : connections.values()) {
      futures[i++] = connection.close();
    }
    return CompletableFuture.allOf(futures);
  }

}
