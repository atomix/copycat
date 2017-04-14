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

/**
 * Event reset request.
 * <p>
 * Reset requests are sent by clients to servers if the client receives an event message out of
 * sequence to force the server to resend events from the correct index.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ResetRequest extends SessionRequest {

  /**
   * Returns a new publish response builder.
   *
   * @return A new publish response builder.
   */
  public static Builder builder() {
    return new Builder(new ResetRequest());
  }

  /**
   * Returns a publish response builder for an existing response.
   *
   * @param response The response to build.
   * @return The publish response builder.
   * @throws NullPointerException if {@code response} is null
   */
  public static Builder builder(ResetRequest response) {
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
    super.readObject(buffer, serializer);
    index = buffer.readLong();
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(index);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), index);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ResetRequest) {
      ResetRequest request = (ResetRequest) object;
      return request.session == session
        && request.index == index;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, index=%d]", getClass().getSimpleName(), session, index);
  }

  /**
   * Reset request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, ResetRequest> {
    protected Builder(ResetRequest response) {
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
      request.index = Assert.argNot(index, index < 0, "index cannot be less than 0");
      return this;
    }

    /**
     * @throws IllegalStateException if sequence is less than 1
     */
    @Override
    public ResetRequest build() {
      super.build();
      Assert.stateNot(request.index < 0, "index cannot be less than 0");
      return request;
    }
  }

}
