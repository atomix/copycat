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

import io.atomix.copycat.protocol.websocket.request.WebSocketKeepAliveRequest;
import io.atomix.copycat.protocol.Address;

import java.util.Collection;

/**
 * Session keep alive response.
 * <p>
 * Session keep alive responses are sent upon the completion of a {@link WebSocketKeepAliveRequest}
 * from a client. Keep alive responses, when successful, provide the current cluster configuration and leader
 * to the client to ensure clients can evolve with the structure of the cluster and make intelligent decisions
 * about connecting to the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface KeepAliveResponse extends SessionResponse {

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
   * Status response builder.
   */
  interface Builder extends SessionResponse.Builder<Builder, KeepAliveResponse> {

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
  }

}
