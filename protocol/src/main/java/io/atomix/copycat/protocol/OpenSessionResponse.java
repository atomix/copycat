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
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;

import java.util.Objects;

/**
 * Open session response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class OpenSessionResponse extends ClientResponse {

  /**
   * Returns a new register client response builder.
   *
   * @return A new register client response builder.
   */
  public static Builder builder() {
    return new Builder(new OpenSessionResponse());
  }

  /**
   * Returns a register client response builder for an existing response.
   *
   * @param response The response to build.
   * @return The register client response builder.
   * @throws NullPointerException if {@code response} is null
   */
  public static Builder builder(OpenSessionResponse response) {
    return new Builder(response);
  }

  private long session;

  /**
   * Returns the registered session ID.
   *
   * @return The registered session ID.
   */
  public long session() {
    return session;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      session = buffer.readLong();
    } else {
      error = CopycatError.forId(buffer.readByte());
      session = 0;
    }
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      buffer.writeLong(session);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, session);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof OpenSessionResponse) {
      OpenSessionResponse response = (OpenSessionResponse) object;
      return response.status == status
        && response.session == session;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, error=%s, session=%d]", getClass().getSimpleName(), status, error, session);
  }

  /**
   * Register response builder.
   */
  public static class Builder extends ClientResponse.Builder<Builder, OpenSessionResponse> {
    protected Builder(OpenSessionResponse response) {
      super(response);
    }

    /**
     * Sets the response session ID.
     *
     * @param session The session ID.
     * @return The register response builder.
     * @throws IllegalArgumentException if {@code session} is less than 1
     */
    public Builder withSession(long session) {
      response.session = Assert.argNot(session, session < 1, "session must be positive");
      return this;
    }
  }

}
