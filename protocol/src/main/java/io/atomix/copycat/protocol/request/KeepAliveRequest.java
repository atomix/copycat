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

import io.atomix.copycat.protocol.websocket.response.WebSocketRegisterResponse;

/**
 * Session keep alive request.
 * <p>
 * Keep alive requests are sent by clients to servers to maintain a session registered via
 * a {@link RegisterRequest}. Once a session has been registered, clients are responsible for sending
 * keep alive requests to the cluster at a rate less than the provided {@link WebSocketRegisterResponse#timeout()}.
 * Keep alive requests also server to acknowledge the receipt of responses and events by the client.
 * The {@link #commandSequence()} number indicates the highest command sequence number for which the client
 * has received a response, and the {@link #eventIndex()} number indicates the highest index for which the
 * client has received an event in proper sequence.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface KeepAliveRequest extends SessionRequest {

  /**
   * Returns the command sequence number.
   *
   * @return The command sequence number.
   */
  long commandSequence();

  /**
   * Returns the event index.
   *
   * @return The event index.
   */
  long eventIndex();

  /**
   * Keep alive request builder.
   */
  interface Builder extends SessionRequest.Builder<Builder, KeepAliveRequest> {

    /**
     * Sets the command sequence number.
     *
     * @param commandSequence The command sequence number.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code commandSequence} is less than 0
     */
    Builder withCommandSequence(long commandSequence);

    /**
     * Sets the event index.
     *
     * @param eventIndex The event index.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code eventIndex} is less than 0
     */
    Builder withEventIndex(long eventIndex);
  }

}
