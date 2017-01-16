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
package io.atomix.copycat.protocol.response;

import io.atomix.copycat.protocol.websocket.request.WebSocketRegisterRequest;
import io.atomix.copycat.protocol.Address;

import java.util.Collection;

/**
 * Session register response.
 * <p>
 * Session register responses are sent in response to {@link WebSocketRegisterRequest}s
 * sent by a client. Upon the successful registration of a session, the register response will contain the
 * registered {@link #session()} identifier, the session {@link #timeout()}, and the current cluster
 * {@link #leader()} and {@link #members()} to allow the client to make intelligent decisions about
 * connecting to and communicating with the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface RegisterResponse extends ProtocolResponse {

  /**
   * Returns the registered session ID.
   *
   * @return The registered session ID.
   */
  long session();

  /**
   * Returns the cluster leader.
   *
   * @return The cluster leader.
   */
  Address leader();

  /**
   * Returns the cluster members.
   *
   * @return The cluster members.
   */
  Collection<Address> members();

  /**
   * Returns the client session timeout.
   *
   * @return The client session timeout.
   */
  long timeout();

  /**
   * Register response builder.
   */
  interface Builder extends ProtocolResponse.Builder<Builder, RegisterResponse> {

    /**
     * Sets the response session ID.
     *
     * @param session The session ID.
     * @return The register response builder.
     * @throws IllegalArgumentException if {@code session} is less than 1
     */
    Builder withSession(long session);

    /**
     * Sets the response leader.
     *
     * @param leader The response leader.
     * @return The response builder.
     */
    Builder withLeader(Address leader);

    /**
     * Sets the response members.
     *
     * @param members The response members.
     * @return The response builder.
     * @throws NullPointerException if {@code members} is null
     */
    Builder withMembers(Collection<Address> members);

    /**
     * Sets the session timeout.
     *
     * @param timeout The session timeout.
     * @return The register response builder.
     * @throws IllegalArgumentException if the session timeout is not positive
     */
    Builder withTimeout(long timeout);
  }

}
