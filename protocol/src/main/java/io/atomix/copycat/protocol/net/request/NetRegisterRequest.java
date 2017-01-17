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
package io.atomix.copycat.protocol.net.request;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.request.RegisterRequest;

import java.util.Objects;

/**
 * Register session request.
 * <p>
 * Clients submit register requests to a Copycat cluster to create a new session. Register requests
 * can be submitted to any server in the cluster and will be proxied to the leader. The session will
 * be associated with the provided {@link #client()} ID such that servers can associate client connections
 * with the session. Once the registration is complete, the client must send {@link NetKeepAliveRequest}s to
 * maintain its session with the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetRegisterRequest extends AbstractNetRequest implements RegisterRequest {
  private final String client;
  private final long timeout;

  protected NetRegisterRequest(long id, String client, long timeout) {
    super(id);
    this.client = client;
    this.timeout = timeout;
  }

  @Override
  public Type type() {
    return Types.REGISTER_REQUEST;
  }

  @Override
  public String client() {
    return client;
  }

  @Override
  public long timeout() {
    return timeout;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), client);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NetRegisterRequest) {
      NetRegisterRequest request = (NetRegisterRequest) object;
      return request.client.equals(client) && request.timeout == timeout;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[client=%s]", getClass().getSimpleName(), client);
  }

  /**
   * Register client request builder.
   */
  public static class Builder extends AbstractNetRequest.Builder<RegisterRequest.Builder, RegisterRequest> implements RegisterRequest.Builder {
    private String client;
    private long timeout;

    public Builder(long id) {
      super(id);
    }

    @Override
    public Builder withClient(String client) {
      this.client = Assert.notNull(client, "client");
      return this;
    }

    @Override
    public Builder withTimeout(long timeout) {
      this.timeout = Assert.arg(timeout, timeout >= -1, "timeout must be -1 or greater");
      return this;
    }

    @Override
    public RegisterRequest build() {
      return new NetRegisterRequest(id, client, timeout);
    }
  }

  /**
   * Register request serializer.
   */
  public static class Serializer extends AbstractNetRequest.Serializer<NetRegisterRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetRegisterRequest request) {
      output.writeLong(request.id);
      output.writeString(request.client);
      output.writeLong(request.timeout);
    }

    @Override
    public NetRegisterRequest read(Kryo kryo, Input input, Class<NetRegisterRequest> type) {
      return new NetRegisterRequest(input.readLong(), input.readString(), input.readLong());
    }
  }
}
