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
package io.atomix.copycat.protocol.tcp.request;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.request.ConnectRequest;

import java.util.Objects;

/**
 * Connect client request.
 * <p>
 * Connect requests are sent by clients to specific servers when first establishing a connection.
 * Connections must be associated with a specific {@link #client() client ID} and must be established
 * each time the client switches servers. A client may only be connected to a single server at any
 * given time.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetSocketConnectRequest extends AbstractNetSocketRequest implements ConnectRequest {
  private final String client;

  protected NetSocketConnectRequest(long id, String client) {
    super(id);
    this.client = client;
  }

  @Override
  public Type type() {
    return Type.CONNECT_REQUEST;
  }

  @Override
  public String client() {
    return client;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), client);
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof NetSocketConnectRequest && ((NetSocketConnectRequest) object).client.equals(client);
  }

  @Override
  public String toString() {
    return String.format("%s[client=%s]", getClass().getSimpleName(), client);
  }

  /**
   * Register client request builder.
   */
  public static class Builder extends AbstractNetSocketRequest.Builder<ConnectRequest.Builder, ConnectRequest> implements ConnectRequest.Builder {
    private String client;

    public Builder(long id) {
      super(id);
    }

    @Override
    public Builder withClient(String client) {
      this.client = Assert.notNull(client, "client");
      return this;
    }

    @Override
    public ConnectRequest build() {
      return new NetSocketConnectRequest(id, client);
    }
  }

  /**
   * Connect request serializer.
   */
  public static class Serializer extends AbstractNetSocketRequest.Serializer<NetSocketConnectRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetSocketConnectRequest request) {
      output.writeLong(request.id);
      output.writeString(request.client);
    }

    @Override
    public NetSocketConnectRequest read(Kryo kryo, Input input, Class<NetSocketConnectRequest> type) {
      return new NetSocketConnectRequest(input.readLong(), input.readString());
    }
  }
}
