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
package io.atomix.copycat.server.protocol.request;

import io.atomix.copycat.server.cluster.Member;

/**
 * Configuration change request.
 * <p>
 * Configuration change requests are the basis for members joining and leaving the cluster.
 * When a member wants to join or leave the cluster, it must submit a configuration change
 * request to the leader where the change will be logged and replicated.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ConfigurationRequest extends RaftProtocolRequest {

  /**
   * Returns the member to configure.
   *
   * @return The member to configure.
   */
  Member member();

  /**
   * Configuration request builder.
   */
  interface Builder<T extends Builder<T, U>, U extends ConfigurationRequest> extends RaftProtocolRequest.Builder<T, U> {

    /**
     * Sets the request member.
     *
     * @param member The request member.
     * @return The request builder.
     * @throws NullPointerException if {@code member} is null
     */
    @SuppressWarnings("unchecked")
    T withMember(Member member);
  }

}
