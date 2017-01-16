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

import java.util.Arrays;
import java.util.List;

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
public interface PublishRequest extends SessionRequest {

  /**
   * Returns the event index.
   *
   * @return The event index.
   */
  long eventIndex();

  /**
   * Returns the previous event index.
   *
   * @return The previous event index.
   */
  long previousIndex();

  /**
   * Returns the request events.
   *
   * @return The request events.
   */
  List<Event<?>> events();

  /**
   * Publish request builder.
   */
  interface Builder extends SessionRequest.Builder<Builder, PublishRequest> {

    /**
     * Sets the event index.
     *
     * @param index The event index.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code index} is less than 1
     */
    Builder withEventIndex(long index);

    /**
     * Sets the previous event index.
     *
     * @param index The previous event index.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code index} is less than 1
     */
    Builder withPreviousIndex(long index);

    /**
     * Sets the request events.
     *
     * @param events The request events.
     * @return The publish request builder.
     */
    default Builder withEvents(Event<?>... events) {
      return withEvents(Arrays.asList(Assert.notNull(events, "events")));
    }

    /**
     * Sets the request events.
     *
     * @param events The request events.
     * @return The publish request builder.
     */
    Builder withEvents(List<Event<?>> events);
  }

}
