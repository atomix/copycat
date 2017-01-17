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

/**
 * Member configuration change request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ReconfigureRequest extends ConfigurationRequest {

  /**
   * Returns the configuration index.
   *
   * @return The configuration index.
   */
  long index();

  /**
   * Returns the configuration term.
   *
   * @return The configuration term.
   */
  long term();

  /**
   * Reconfigure request builder.
   */
  interface Builder extends ConfigurationRequest.Builder<Builder, ReconfigureRequest> {

    /**
     * Sets the request index.
     *
     * @param index The request index.
     * @return The request builder.
     */
    Builder withIndex(long index);

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The request builder.
     */
    Builder withTerm(long term);
  }

}
