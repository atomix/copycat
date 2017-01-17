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
package io.atomix.copycat.server.protocol.response;

/**
 * Configuration installation response.
 * <p>
 * Configuration installation responses are sent in response to configuration installation requests to
 * indicate the simple success of the installation of a configuration. If the response {@link #status()}
 * is {@link Status#OK} then the installation was successful.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface ConfigureResponse extends RaftProtocolResponse {

  /**
   * Heartbeat response builder.
   */
  interface Builder extends RaftProtocolResponse.Builder<Builder, ConfigureResponse> {
  }

}
