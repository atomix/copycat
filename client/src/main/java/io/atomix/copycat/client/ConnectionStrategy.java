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
 * limitations under the License
 */
package io.atomix.copycat.client;

import io.atomix.catalyst.transport.Address;

import java.util.List;

/**
 * Strategy for managing client connections.
 * <p>
 * Connection strategies are responsible for defining the servers to which a client can connect.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ConnectionStrategy {

  /**
   * Returns a list of servers to which the client can connect.
   *
   * @param server The server to which to connect.
   * @return A collection of servers to which the client can connect.
   */
  boolean connect(Address server, Address leader);

}
