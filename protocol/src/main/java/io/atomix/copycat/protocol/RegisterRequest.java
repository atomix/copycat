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
package io.atomix.copycat.protocol;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;

import java.util.Objects;
import java.util.UUID;

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
   * Returns a new register client request builder.
   *
   * @return A new register client request builder.
   */
  public static Builder builder() {
    return new Builder(new RegisterRequest());
  }

  /**
   * Returns a register client request builder for an existing request.
   *
   * @param request The request to build.
   * @return The register client request builder.
   * @throws NullPointerException if {@code request} is null
   */
  public static Builder builder(RegisterRequest request) {
    return new Builder(request);
  }

  private UUID client;

  /**
   * Returns the client ID.
   *
   * @return The client ID.
   */
  public UUID client() {
    return client;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeString(client.toString());
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    client = UUID.fromString(buffer.readString());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), client);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof RegisterRequest) {
      RegisterRequest request = (RegisterRequest) object;
      return request.client.equals(client);
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
  public static class Builder extends AbstractRequest.Builder<Builder, RegisterRequest> {
    protected Builder(RegisterRequest request) {
      super(request);
    }

    /**
     * Sets the client ID.
     *
     * @param client The client ID.
     * @return The request builder.
     * @throws NullPointerException if {@code client} is null
     */
    public Builder withClient(UUID client) {
      request.client = Assert.notNull(client, "client");
      return this;
    }

    /**
     * @throws IllegalStateException if client is null
     */
    @Override
    public RegisterRequest build() {
      super.build();
      Assert.stateNot(request.client == null, "client cannot be null");
      return request;
    }
  }

}
