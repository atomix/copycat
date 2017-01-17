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
package io.atomix.copycat.server.protocol.net.request;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.net.request.AbstractNetRequest;
import io.atomix.copycat.server.protocol.request.AcceptRequest;

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
public class NetAcceptRequest extends AbstractNetRequest implements AcceptRequest, RaftNetRequest {
  private final String client;
  private final Address address;

  protected NetAcceptRequest(long id, String client, Address address) {
    super(id);
    this.client = client;
    this.address = address;
  }

  @Override
  public Type type() {
    return RaftNetRequest.Types.ACCEPT_REQUEST;
  }

  @Override
  public String client() {
    return client;
  }

  @Override
  public Address address() {
    return address;
  }

  /**
   * Register client request builder.
   */
  public static class Builder extends AbstractNetRequest.Builder<AcceptRequest.Builder, AcceptRequest> implements AcceptRequest.Builder {
    private String client;
    private Address address;

    public Builder(long id) {
      super(id);
    }

    @Override
    public Builder withClient(String client) {
      this.client = Assert.notNull(client, "client");
      return this;
    }

    @Override
    public Builder withAddress(Address address) {
      this.address = Assert.notNull(address, "address");
      return this;
    }

    @Override
    public NetAcceptRequest build() {
      return new NetAcceptRequest(id, client, address);
    }
  }

}
