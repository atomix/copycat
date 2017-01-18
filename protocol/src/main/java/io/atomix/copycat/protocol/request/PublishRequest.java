/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.copycat.protocol.request;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.session.Event;

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
  protected final long eventIndex;
  protected final long previousIndex;
  protected final List<Event<?>> events;

  protected PublishRequest(long session, long eventIndex, long previousIndex, List<Event<?>> events) {
    super(session);
    this.eventIndex = Assert.argNot(eventIndex, eventIndex < 1, "eventIndex cannot be less than 1");
    this.previousIndex = Assert.argNot(previousIndex, previousIndex < 0, "previousIndex cannot be less than 0");
    this.events = Assert.notNull(events, "events");
  }

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
  public static class Builder extends SessionRequest.Builder<PublishRequest.Builder, PublishRequest> {
    protected long eventIndex;
    protected long previousIndex;
    protected List<Event<?>> events;

    /**
     * Sets the event index.
     *
     * @param index The event index.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code index} is less than 1
     */
    public Builder withEventIndex(long index) {
      this.eventIndex = Assert.argNot(index, index < 1, "index cannot be less than 1");
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
      this.previousIndex = Assert.argNot(index, index < 0, "index cannot be less than 0");
      return this;
    }

    /**
     * Sets the request events.
     *
     * @param events The request events.
     * @return The publish request builder.
     */
    public Builder withEvents(List<Event<?>> events) {
      this.events = Assert.notNull(events, "events");
      return this;
    }

    @Override
    public PublishRequest copy(PublishRequest request) {
      return new PublishRequest(request.session, request.eventIndex, request.previousIndex, request.events);
    }

    @Override
    public PublishRequest build() {
      return new PublishRequest(session, eventIndex, previousIndex, events);
    }
  }
}
