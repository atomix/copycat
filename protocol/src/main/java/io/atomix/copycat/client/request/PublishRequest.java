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
package io.atomix.copycat.client.request;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.Event;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Protocol publish request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=205)
public class PublishRequest extends SessionRequest<PublishRequest> {

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

  private long eventVersion;
  private long previousVersion;
  private List<Event<?>> events = new ArrayList<>(8);

  /**
   * Returns the event version number.
   *
   * @return The event version number.
   */
  public long eventVersion() {
    return eventVersion;
  }

  /**
   * Returns the previous event version number.
   *
   * @return The previous event version number.
   */
  public long previousVersion() {
    return previousVersion;
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
    eventVersion = buffer.readLong();
    previousVersion = buffer.readLong();

    events.clear();
    int size = buffer.readUnsignedShort();
    for (int i = 0; i < size; i++) {
      events.add(serializer.readObject(buffer));
    }
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(eventVersion);
    buffer.writeLong(previousVersion);

    buffer.writeUnsignedShort(events.size());
    for (Event<?> event : events) {
      serializer.writeObject(event, buffer);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, eventVersion, previousVersion, events);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PublishRequest) {
      PublishRequest request = (PublishRequest) object;
      return request.session == session
        && request.eventVersion == eventVersion
        && request.previousVersion == previousVersion
        && request.events.equals(events);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, eventVersion=%d, previousVersion=%d, events=%s]", getClass().getSimpleName(), session, eventVersion, previousVersion, events);
  }

  /**
   * Publish request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, PublishRequest> {
    protected Builder(PublishRequest request) {
      super(request);
    }

    /**
     * Sets the event version number.
     *
     * @param version The event version number.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code version} is less than 1
     */
    public Builder withEventVersion(long version) {
      request.eventVersion = Assert.argNot(version, version < 1, "version cannot be less than 1");
      return this;
    }

    /**
     * Sets the previous event version number.
     *
     * @param version The previous event version number.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code version} is less than 1
     */
    public Builder withPreviousVersion(long version) {
      request.previousVersion = Assert.argNot(version, version < 0, "version cannot be less than 0");
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
      Assert.stateNot(request.eventVersion < 0, "eventVersion cannot be less than 0");
      Assert.stateNot(request.previousVersion < -1, "previousVersion cannot be less than -1");
      Assert.stateNot(request.events == null, "events cannot be null");
      return request;
    }
  }

}
