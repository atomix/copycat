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
package io.atomix.copycat.server.protocol;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.AbstractResponse;
import io.atomix.copycat.protocol.Response;

import java.util.Objects;

/**
 * Server accept client response.
 * <p>
 * Accept client responses are sent to between servers once a new
 * {@link io.atomix.copycat.server.storage.entry.ConnectEntry} has been committed to the Raft log, denoting
 * the relationship between a client and server. If the acceptance of the connection was successful, the
 * response status will be {@link Response.Status#OK}, otherwise an error
 * will be provided.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AcceptResponse extends AbstractResponse {

  /**
   * Returns a new accept client response builder.
   *
   * @return A new accept client response builder.
   */
  public static Builder builder() {
    return new Builder(new AcceptResponse());
  }

  /**
   * Returns a accept client response builder for an existing response.
   *
   * @param response The response to build.
   * @return The accept client response builder.
   * @throws NullPointerException if {@code response} is null
   */
  public static Builder builder(AcceptResponse response) {
    return new Builder(response);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
    } else {
      error = CopycatError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
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
    if (object instanceof AcceptResponse) {
      AcceptResponse response = (AcceptResponse) object;
      return response.status == status;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s]", getClass().getSimpleName(), status);
  }

  /**
   * Register response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, AcceptResponse> {
    protected Builder(AcceptResponse response) {
      super(response);
    }
  }

}
