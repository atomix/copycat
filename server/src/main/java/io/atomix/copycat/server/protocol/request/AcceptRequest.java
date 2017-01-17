/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.copycat.server.protocol.request;

import io.atomix.catalyst.transport.Address;

/**
 * Accept client request.
 * <p>
 * Accept requests are sent by followers to the leader to log and replicate the connection of
 * a specific client to a specific server. The {@link #address()} in the accept request indicates
 * the server to which the client is connected. Accept requests will ultimately result in a
 * {@link io.atomix.copycat.server.storage.entry.ConnectEntry} being logged and replicated such
 * that all server state machines receive updates on the relationships between clients and servers.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface AcceptRequest extends RaftProtocolRequest {

  /**
   * Returns the accepted client ID.
   *
   * @return The accepted client ID.
   */
  String client();

  /**
   * Returns the accept server address.
   *
   * @return The accept server address.
   */
  Address address();

  /**
   * Register client request builder.
   */
  interface Builder extends RaftProtocolRequest.Builder<Builder, AcceptRequest> {

    /**
     * Sets the request client.
     *
     * @param client The request client.
     * @return The request builder.
     */
    Builder withClient(String client);

    /**
     * Sets the request address.
     *
     * @param address The request address.
     * @return The request builder.
     */
    Builder withAddress(Address address);
  }

}
