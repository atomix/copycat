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

import io.atomix.copycat.protocol.Response;

/**
 * Server leave configuration change response.
 * <p>
 * Leave responses are sent in response to a request to add a server to the cluster configuration. If a
 * configuration change is failed due to a conflict, the response status will be
 * {@link Response.Status#ERROR} but the response {@link #error()} will
 * be {@code null}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaveResponse extends ConfigurationResponse {

  /**
   * Returns a new leave response builder.
   *
   * @return A new leave response builder.
   */
  public static Builder builder() {
    return new Builder(new LeaveResponse());
  }

  /**
   * Returns an leave response builder for an existing response.
   *
   * @param response The response to build.
   * @return The leave response builder.
   */
  public static Builder builder(LeaveResponse response) {
    return new Builder(response);
  }

  /**
   * Leave response builder.
   */
  public static class Builder extends ConfigurationResponse.Builder<Builder, LeaveResponse> {
    protected Builder(LeaveResponse response) {
      super(response);
    }
  }

}
