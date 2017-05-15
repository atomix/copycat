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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Client manager.
 */
public class ClientManager {
  private final Map<Long, ClientContext> clients = new ConcurrentHashMap<>();

  /**
   * Returns a client by ID.
   *
   * @param client The client ID.
   * @return The client context.
   */
  public ClientContext getClient(long client) {
    return clients.get(client);
  }

  /**
   * Returns the collection of registered clients.
   *
   * @return The collection of registered clients.
   */
  public Collection<ClientContext> getClients() {
    return clients.values();
  }

  /**
   * Registers a client.
   *
   * @param client The client to register.
   */
  void registerClient(ClientContext client) {
    clients.put(client.id(), client);
  }

  /**
   * Unregisters a client.
   *
   * @param client The client ID.
   * @return The unregistered client context.
   */
  ClientContext unregisterClient(long client) {
    return clients.remove(client);
  }

}
