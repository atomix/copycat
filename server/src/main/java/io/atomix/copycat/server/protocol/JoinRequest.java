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
package io.atomix.copycat.server.protocol;

/**
 * Server join configuration change request.
 * <p>
 * The join request is the mechanism by which new servers join a cluster. When a server wants to
 * join a cluster, it must submit a join request to the leader. The leader will attempt to commit
 * the configuration change and, if successful, respond to the join request with the updated configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class JoinRequest extends ConfigurationRequest {

  /**
   * Returns a new join request builder.
   *
   * @return A new join request builder.
   */
  public static Builder builder() {
    return new Builder(new JoinRequest());
  }

  /**
   * Returns an join request builder for an existing request.
   *
   * @param request The request to build.
   * @return The join request builder.
   */
  public static Builder builder(JoinRequest request) {
    return new Builder(request);
  }

  /**
   * Join request builder.
   */
  public static class Builder extends ConfigurationRequest.Builder<Builder, JoinRequest> {
    protected Builder(JoinRequest request) {
      super(request);
    }
  }

}
