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
 * Session keep alive request.
 * <p>
 * Keep alive requests are sent by clients to servers to maintain a session registered via
 * a {@link RegisterRequest}. Once a session has been registered, clients are responsible for sending
 * keep alive requests to the cluster at a rate less than the provided {@link RegisterResponse#timeout()}.
 * Keep alive requests also server to acknowledge the receipt of responses and events by the client.
 * The {@link #commandSequence()} number indicates the highest command sequence number for which the client
 * has received a response, and the {@link #eventIndex()} number indicates the highest index for which the
 * client has received an event in proper sequence.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KeepAliveRequest extends SessionRequest {

  /**
   * Returns a new keep alive request builder.
   *
   * @return A new keep alive request builder.
   */
  public static Builder builder() {
    return new Builder(new KeepAliveRequest());
  }

  /**
   * Returns a keep alive request builder for an existing request.
   *
   * @param request The request to build.
   * @return The keep alive request builder.
   * @throws NullPointerException if {@code request} is null
   */
  public static Builder builder(KeepAliveRequest request) {
    return new Builder(request);
  }

  private long commandSequence;
  private long eventIndex;

  /**
   * Returns the command sequence number.
   *
   * @return The command sequence number.
   */
  public long commandSequence() {
    return commandSequence;
  }

  /**
   * Returns the event index.
   *
   * @return The event index.
   */
  public long eventIndex() {
    return eventIndex;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    commandSequence = buffer.readLong();
    eventIndex = buffer.readLong();
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(commandSequence);
    buffer.writeLong(eventIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, commandSequence);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof KeepAliveRequest) {
      KeepAliveRequest request = (KeepAliveRequest) object;
      return request.session == session
        && request.commandSequence == commandSequence
        && request.eventIndex == eventIndex;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, commandSequence=%d, eventIndex=%d]", getClass().getSimpleName(), session, commandSequence, eventIndex);
  }

  /**
   * Keep alive request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, KeepAliveRequest> {
    protected Builder(KeepAliveRequest request) {
      super(request);
    }

    /**
     * Sets the command sequence number.
     *
     * @param commandSequence The command sequence number.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code commandSequence} is less than 0
     */
    public Builder withCommandSequence(long commandSequence) {
      request.commandSequence = Assert.argNot(commandSequence, commandSequence < 0, "commandSequence cannot be negative");
      return this;
    }

    /**
     * Sets the event index.
     *
     * @param eventIndex The event index.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code eventIndex} is less than 0
     */
    public Builder withEventIndex(long eventIndex) {
      request.eventIndex = Assert.argNot(eventIndex, eventIndex < 0, "eventIndex cannot be negative");
      return this;
    }

    /**
     * @throws IllegalStateException is session is not positive
     */
    @Override
    public KeepAliveRequest build() {
      super.build();
      Assert.state(request.session > 0, "session must be positive");
      return request;
    }
  }

}
