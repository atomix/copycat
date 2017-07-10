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
package io.atomix.copycat.protocol;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializationException;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;

import java.util.Objects;

/**
 * Open session request.
 */
public class OpenSessionRequest extends ClientRequest {
  public static final String NAME = "open-session";

  /**
   * Returns a new open session request builder.
   *
   * @return A new open session request builder.
   */
  public static Builder builder() {
    return new Builder(new OpenSessionRequest());
  }

  /**
   * Returns an open session request builder for an existing request.
   *
   * @param request The request to build.
   * @return The open session request builder.
   * @throws NullPointerException if {@code request} is null
   */
  public static Builder builder(OpenSessionRequest request) {
    return new Builder(request);
  }

  private String name;
  private String type;

  /**
   * Returns the state machine name.
   *
   * @return The state machine name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the state machine type;
   *
   * @return The state machine type.
   */
  public String type() {
    return type;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    name = buffer.readString();
    type = buffer.readString();
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeString(name);
    buffer.writeString(type);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), name, type);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof OpenSessionRequest) {
      OpenSessionRequest request = (OpenSessionRequest) object;
      return request.client == client
        && request.name.equals(name)
        && request.type.equals(type);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[client=%s, name=%s, type=%s]", getClass().getSimpleName(), client, name, type);
  }

  /**
   * Open session request builder.
   */
  public static class Builder extends ClientRequest.Builder<Builder, OpenSessionRequest> {
    protected Builder(OpenSessionRequest request) {
      super(request);
    }

    /**
     * Sets the state machine name.
     *
     * @param name The state machine name.
     * @return The open session request builder.
     * @throws NullPointerException if {@code name} is {@code null}
     */
    public Builder withName(String name) {
      request.name = Assert.notNull(name, "name");
      return this;
    }

    /**
     * Sets the state machine type.
     *
     * @param type The state machine type.
     * @return The open session request builder.
     * @throws NullPointerException if {@code type} is {@code null}
     */
    public Builder withType(String type) {
      request.type = Assert.notNull(type, "type");
      return this;
    }

    /**
     * @throws IllegalStateException is session is not positive
     */
    @Override
    public OpenSessionRequest build() {
      super.build();
      Assert.notNull(request.name, "name");
      Assert.notNull(request.type, "type");
      return request;
    }
  }
}
