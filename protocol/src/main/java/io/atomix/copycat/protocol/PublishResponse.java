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
import io.atomix.copycat.error.CopycatError;

import java.util.Objects;

/**
 * Event publish response.
 * <p>
 * Publish responses are sent by clients to servers to indicate the last successful index for which
 * an event message was handled in proper sequence. If the client receives an event message out of
 * sequence, it should respond with the index of the last event it received in sequence. If an event
 * message is received in sequence, it should respond with the index of that event. Once a client has
 * responded successfully to an event message, it will be removed from memory on the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PublishResponse extends SessionResponse {

  /**
   * Returns a new publish response builder.
   *
   * @return A new publish response builder.
   */
  public static Builder builder() {
    return new Builder(new PublishResponse());
  }

  /**
   * Returns a publish response builder for an existing response.
   *
   * @param response The response to build.
   * @return The publish response builder.
   * @throws NullPointerException if {@code response} is null
   */
  public static Builder builder(PublishResponse response) {
    return new Builder(response);
  }

  private long index;

  /**
   * Returns the event index.
   *
   * @return The event index.
   */
  public long index() {
    return index;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
    } else if (buffer.readBoolean()) {
      error = CopycatError.forId(buffer.readByte());
    }
    index = buffer.readLong();
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.ERROR) {
      if (error != null) {
        buffer.writeBoolean(true).writeByte(error.id());
      } else {
        buffer.writeBoolean(false);
      }
    }
    buffer.writeLong(index);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PublishResponse) {
      PublishResponse response = (PublishResponse) object;
      return response.status == status
        && response.index == index;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, index=%d]", getClass().getSimpleName(), status, index);
  }

  /**
   * Publish response builder.
   */
  public static class Builder extends SessionResponse.Builder<Builder, PublishResponse> {
    protected Builder(PublishResponse response) {
      super(response);
    }

    /**
     * Sets the event index.
     *
     * @param index The event index.
     * @return The response builder.
     * @throws IllegalArgumentException if {@code index} is less than {@code 1}
     */
    public Builder withIndex(long index) {
      response.index = Assert.argNot(index, index < 0, "index cannot be less than 0");
      return this;
    }

    /**
     * @throws IllegalStateException if sequence is less than 1
     */
    @Override
    public PublishResponse build() {
      super.build();
      Assert.stateNot(response.index < 0, "index cannot be less than 0");
      return response;
    }
  }

}
