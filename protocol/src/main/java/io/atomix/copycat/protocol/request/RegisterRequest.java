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

/**
 * Register session request.
 * <p>
 * Clients submit register requests to a Copycat cluster to create a new session. Register requests
 * can be submitted to any server in the cluster and will be proxied to the leader. The session will
 * be associated with the provided {@link #client()} ID such that servers can associate client connections
 * with the session. Once the registration is complete, the client must send {@link KeepAliveRequest}s to
 * maintain its session with the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface RegisterRequest extends ProtocolRequest {

  /**
   * Returns the client ID.
   *
   * @return The client ID.
   */
  String client();

  /**
   * Returns the client session timeout.
   *
   * @return The client session timeout.
   */
  long timeout();

  /**
   * Register client request builder.
   */
  interface Builder extends ProtocolRequest.Builder<Builder, RegisterRequest> {

    /**
     * Sets the client ID.
     *
     * @param client The client ID.
     * @return The request builder.
     * @throws NullPointerException if {@code client} is null
     */
    Builder withClient(String client);

    /**
     * Sets the client session timeout.
     *
     * @param timeout The client session timeout.
     * @return The request builder.
     * @throws IllegalArgumentException if the timeout is not {@code -1} or a positive number
     */
    Builder withTimeout(long timeout);
  }

}
