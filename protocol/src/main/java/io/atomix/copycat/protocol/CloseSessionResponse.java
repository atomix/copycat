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
import io.atomix.copycat.error.CopycatError;

import java.util.Objects;

/**
 * Close session response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CloseSessionResponse extends SessionResponse {

  /**
   * Returns a new keep alive response builder.
   *
   * @return A new keep alive response builder.
   */
  public static Builder builder() {
    return new Builder(new CloseSessionResponse());
  }

  /**
   * Returns a keep alive response builder for an existing response.
   *
   * @param response The response to build.
   * @return The keep alive response builder.
   * @throws NullPointerException if {@code response} is null
   */
  public static Builder builder(CloseSessionResponse response) {
    return new Builder(response);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
    } else {
      error = CopycatError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.ERROR) {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof CloseSessionResponse) {
      CloseSessionResponse response = (CloseSessionResponse) object;
      return response.status == status;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, error=%s]", getClass().getSimpleName(), status, error);
  }

  /**
   * Status response builder.
   */
  public static class Builder extends SessionResponse.Builder<Builder, CloseSessionResponse> {
    protected Builder(CloseSessionResponse response) {
      super(response);
    }
  }

}
