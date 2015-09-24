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
package io.atomix.catalog.client.request;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.BuilderPool;
import io.atomix.catalyst.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol publish request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=266)
public class PublishRequest extends SessionRequest<PublishRequest> {

  private static final BuilderPool<Builder, PublishRequest> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new publish request builder.
   *
   * @return A new publish request builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a publish request builder for an existing request.
   *
   * @param request The request to build.
   * @return The publish request builder.
   * @throws NullPointerException if {@code request} is null
   */
  public static Builder builder(PublishRequest request) {
    return POOL.acquire(Assert.notNull(request, "request"));
  }

  private long eventVersion;
  private long eventSequence;
  private long previousVersion;
  private long previousSequence;
  private String event;
  private Object message;

  /**
   * @throws NullPointerException if {@code referenceManager} is null
   */
  public PublishRequest(ReferenceManager<PublishRequest> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the event version number.
   *
   * @return The event version number.
   */
  public long eventVersion() {
    return eventVersion;
  }

  /**
   * Returns the event sequence number.
   *
   * @return The event sequence number.
   */
  public long eventSequence() {
    return eventSequence;
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
   * Returns the previous event sequence number.
   *
   * @return The previous event sequence number.
   */
  public long previousSequence() {
    return previousSequence;
  }

  /**
   * Returns the request event.
   *
   * @return The request event.
   */
  public String event() {
    return event;
  }

  /**
   * Returns the request message.
   *
   * @return The request message.
   */
  public Object message() {
    return message;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    eventVersion = buffer.readLong();
    eventSequence = buffer.readLong();
    previousVersion = buffer.readLong();
    previousSequence = buffer.readLong();
    event = buffer.readString();
    message = serializer.readObject(buffer);
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(eventVersion);
    buffer.writeLong(eventSequence);
    buffer.writeLong(previousVersion);
    buffer.writeLong(previousSequence);
    buffer.writeString(event);
    serializer.writeObject(message, buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, eventVersion, eventSequence, previousVersion, previousSequence, event, message);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof PublishRequest) {
      PublishRequest request = (PublishRequest) object;
      return request.session == session
        && request.eventVersion == eventVersion
        && request.eventSequence == eventSequence
        && request.previousVersion == previousVersion
        && request.previousSequence == previousSequence
        && request.event.equals(event)
        && request.message.equals(message);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, eventVersion=%d, eventSequence=%d, previousVersion=%d, previousSequence=%d, event=%s, message=%s]", getClass().getSimpleName(), session, eventVersion, eventSequence, previousVersion, previousSequence, event, message);
  }

  /**
   * Publish request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, PublishRequest> {
    /**
     * @throws NullPointerException if {@code pool} is null
     */
    protected Builder(BuilderPool<Builder, PublishRequest> pool) {
      super(pool, PublishRequest::new);
    }

    @Override
    protected void reset() {
      super.reset();
      request.eventVersion = 0;
      request.eventSequence = 0;
      request.previousVersion = -1;
      request.previousSequence = -1;
      request.event = null;
      request.message = null;
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
     * Sets the event sequence number.
     *
     * @param sequence The event sequence number.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code sequence} is less than 1
     */
    public Builder withEventSequence(long sequence) {
      request.eventSequence = Assert.argNot(sequence, sequence < 1, "sequence cannot be less than 1");
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
     * Sets the previous event sequence number.
     *
     * @param sequence The previous event sequence number.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code sequence} is less than 1
     */
    public Builder withPreviousSequence(long sequence) {
      request.previousSequence = Assert.argNot(sequence, sequence < 0, "sequence cannot be less than 0");
      return this;
    }

    /**
     * Sets the request event.
     *
     * @param event The request event.
     * @return The publish request builder.
     */
    public Builder withEvent(String event) {
      request.event = Assert.notNull(event, "event");
      return this;
    }

    /**
     * Sets the request message.
     *
     * @param message The request message.
     * @return The publish request builder.
     * @throws NullPointerException if {@code message} is null
     */
    public Builder withMessage(Object message) {
      request.message = Assert.notNull(message, "message");
      return this;
    }

    /**
     * @throws IllegalStateException if sequence is less than 1 or message is null
     */
    @Override
    public PublishRequest build() {
      super.build();
      Assert.stateNot(request.eventVersion < 1, "eventVersion cannot be less than 1");
      Assert.stateNot(request.eventSequence < 1, "eventSequence cannot be less than 1");
      Assert.stateNot(request.previousVersion < 0, "previousVersion cannot be less than 0");
      Assert.stateNot(request.previousSequence < 0, "previousSequence cannot be less than 0");
      Assert.stateNot(request.event == null, "event cannot be null");
      Assert.stateNot(request.message == null, "message cannot be null");
      return request;
    }
  }

}
