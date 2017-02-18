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
package io.atomix.copycat.protocol.request;

import io.atomix.copycat.util.Assert;

import java.util.Objects;

/**
 * Register session request.
 * <p>
 * Clients submit register requests to a Copycat cluster to create a new session. Register requests
 * can be submitted to any server in the cluster and will be proxied to the leader. The session will
 * be associated with the provided {@link #client()} ID such that servers can associate client connections
 * with the session. Once the registration is complete, the client must send {@link KeepAliveRequest}s to
 * maintain its session with the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class RegisterRequest extends AbstractRequest {

  /**
   * Returns a new register request builder.
   *
   * @return A new register request builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  protected final String client;
  protected final long timeout;

  public RegisterRequest(String client, long timeout) {
    this.client = Assert.notNull(client, "client");
    this.timeout = Assert.arg(timeout, timeout >= -1, "timeout must be -1 or greater");
  }

  @Override
  public Type type() {
    return Type.REGISTER;
  }

  /**
   * Returns the client ID.
   *
   * @return The client ID.
   */
  public String client() {
    return client;
  }

  /**
   * Returns the client session timeout.
   *
   * @return The client session timeout.
   */
  public long timeout() {
    return timeout;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), client);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof RegisterRequest) {
      RegisterRequest request = (RegisterRequest) object;
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
  public static class Builder extends AbstractRequest.Builder<RegisterRequest.Builder, RegisterRequest> {
    protected String client;
    protected long timeout;

    /**
     * Sets the client ID.
     *
     * @param client The client ID.
     * @return The request builder.
     * @throws NullPointerException if {@code client} is null
     */
    public Builder withClient(String client) {
      this.client = Assert.notNull(client, "client");
      return this;
    }

    /**
     * Sets the client session timeout.
     *
     * @param timeout The client session timeout.
     * @return The request builder.
     * @throws IllegalArgumentException if the timeout is not {@code -1} or a positive number
     */
    public Builder withTimeout(long timeout) {
      this.timeout = Assert.arg(timeout, timeout >= -1, "timeout must be -1 or greater");
      return this;
    }

    @Override
    public RegisterRequest copy(RegisterRequest request) {
      return new RegisterRequest(request.client, request.timeout);
    }

    @Override
    public RegisterRequest build() {
      return new RegisterRequest(client, timeout);
    }
  }
}
