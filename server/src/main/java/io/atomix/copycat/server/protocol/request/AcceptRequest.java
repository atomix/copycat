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

import io.atomix.copycat.util.Assert;
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.request.AbstractRequest;

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
public class AcceptRequest extends AbstractRequest implements RaftProtocolRequest {

  /**
   * Returns a new accept request builder.
   *
   * @return A new accept request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  protected final String client;
  protected final Address address;

  public AcceptRequest(String client, Address address) {
    this.client = Assert.notNull(client, "client");
    this.address = Assert.notNull(address, "address");
  }

  @Override
  public RaftProtocolRequest.Type type() {
    return RaftProtocolRequest.Type.ACCEPT;
  }

  /**
   * Returns the accepted client ID.
   *
   * @return The accepted client ID.
   */
  public String client() {
    return client;
  }

  /**
   * Returns the accept server address.
   *
   * @return The accept server address.
   */
  public Address address() {
    return address;
  }

  /**
   * Register client request builder.
   */
  public static class Builder extends AbstractRequest.Builder<AcceptRequest.Builder, AcceptRequest> {
    protected String client;
    protected Address address;

    /**
     * Sets the request client.
     *
     * @param client The request client.
     * @return The request builder.
     */
    public Builder withClient(String client) {
      this.client = Assert.notNull(client, "client");
      return this;
    }

    /**
     * Sets the request address.
     *
     * @param address The request address.
     * @return The request builder.
     */
    public Builder withAddress(Address address) {
      this.address = Assert.notNull(address, "address");
      return this;
    }

    @Override
    public AcceptRequest copy(AcceptRequest request) {
      return new AcceptRequest(request.client, request.address);
    }

    @Override
    public AcceptRequest build() {
      return new AcceptRequest(client, address);
    }
  }
}
