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
package io.atomix.copycat.server.request;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.request.SessionRequest;

import java.util.Objects;

/**
 * Protocol accept client request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=215)
public class AcceptRequest extends SessionRequest<AcceptRequest> {

  /**
   * Returns a new accept client request builder.
   *
   * @return A new accept client request builder.
   */
  public static Builder builder() {
    return new Builder(new AcceptRequest());
  }

  /**
   * Returns a accept client request builder for an existing request.
   *
   * @param request The request to build.
   * @return The accept client request builder.
   * @throws NullPointerException if {@code request} is null
   */
  public static Builder builder(AcceptRequest request) {
    return new Builder(request);
  }

  private Address address;

  /**
   * Returns the accept server address.
   *
   * @return The accept server address.
   */
  public Address address() {
    return address;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    address = serializer.readObject(buffer);
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    serializer.writeObject(address, buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, address);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof AcceptRequest) {
      AcceptRequest request = (AcceptRequest) object;
      return request.session == session && request.address.equals(address);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, address=%s]", getClass().getSimpleName(), session, address);
  }

  /**
   * Register client request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, AcceptRequest> {
    protected Builder(AcceptRequest request) {
      super(request);
    }

    /**
     * Sets the request address.
     *
     * @param address The request address.
     * @return The request builder.
     */
    public Builder withAddress(Address address) {
      request.address = Assert.notNull(address, "address");
      return this;
    }

    @Override
    public AcceptRequest build() {
      super.build();
      Assert.stateNot(request.address == null, "address cannot be null");
      return request;
    }
  }

}
