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
import io.atomix.copycat.session.Event;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Event publish request.
 * <p>
 * Publish requests are used by servers to publish event messages to clients. Event messages are
 * sequenced based on the point in the Raft log at which they were published to the client. The
 * {@link #eventIndex()} indicates the index at which the event was sent, and the {@link #previousIndex()}
 * indicates the index of the prior event messages sent to the client. Clients must ensure that event
 * messages are received in sequence by tracking the last index for which they received an event message
 * and validating {@link #previousIndex()} against that index.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class PublishRequest extends SessionRequest {

  /**
   * Returns a new publish request builder.
   *
   * @return A new publish request builder.
   */
  public static Builder builder() {
    return new Builder(new PublishRequest());
  }

  /**
   * Returns a publish request builder for an existing request.
   *
   * @param request The request to build.
   * @return The publish request builder.
   * @throws NullPointerException if {@code request} is null
   */
  public static Builder builder(PublishRequest request) {
    return new Builder(request);
  }

  private long eventIndex;
  private long previousIndex;
  private List<Event<?>> events = new ArrayList<>(8);

  /**
   * Returns the event index.
   *
   * @return The event index.
   */
  public long eventIndex() {
    return eventIndex;
  }

  /**
   * Returns the previous event index.
   *
   * @return The previous event index.
   */
  public long previousIndex() {
    return previousIndex;
  }

  /**
   * Returns the request events.
   *
   * @return The request events.
   */
  public List<Event<?>> events() {
    return events;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    eventIndex = buffer.readLong();
    previousIndex = buffer.readLong();

    events.clear();
    int size = buffer.readUnsignedShort();
    for (int i = 0; i < size; i++) {
      events.add(serializer.readObject(buffer));
    }
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(eventIndex);
    buffer.writeLong(previousIndex);

    buffer.writeUnsignedShort(events.size());
    for (Event<?> event : events) {
      serializer.writeObject(event, buffer);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, eventIndex, previousIndex, events);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PublishRequest) {
      PublishRequest request = (PublishRequest) object;
      return request.session == session
        && request.eventIndex == eventIndex
        && request.previousIndex == previousIndex
        && request.events.equals(events);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, eventIndex=%d, previousIndex=%d, events=%s]", getClass().getSimpleName(), session, eventIndex, previousIndex, events);
  }

  /**
   * Publish request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, PublishRequest> {
    protected Builder(PublishRequest request) {
      super(request);
    }

    /**
     * Sets the event index.
     *
     * @param index The event index.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code index} is less than 1
     */
    public Builder withEventIndex(long index) {
      request.eventIndex = Assert.argNot(index, index < 1, "index cannot be less than 1");
      return this;
    }

    /**
     * Sets the previous event index.
     *
     * @param index The previous event index.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code index} is less than 1
     */
    public Builder withPreviousIndex(long index) {
      request.previousIndex = Assert.argNot(index, index < 0, "index cannot be less than 0");
      return this;
    }

    /**
     * Sets the request events.
     *
     * @param events The request events.
     * @return The publish request builder.
     */
    public Builder withEvents(Event<?>... events) {
      return withEvents(Arrays.asList(Assert.notNull(events, "events")));
    }

    /**
     * Sets the request events.
     *
     * @param events The request events.
     * @return The publish request builder.
     */
    public Builder withEvents(List<Event<?>> events) {
      request.events = events;
      return this;
    }

    /**
     * @throws IllegalStateException if sequence is less than 1 or message is null
     */
    @Override
    public PublishRequest build() {
      super.build();
      Assert.stateNot(request.eventIndex < 0, "eventIndex cannot be less than 0");
      Assert.stateNot(request.previousIndex < -1, "previousIndex cannot be less than -1");
      Assert.stateNot(request.events == null, "events cannot be null");
      return request;
    }
  }

}
