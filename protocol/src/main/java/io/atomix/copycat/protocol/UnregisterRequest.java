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
package io.atomix.copycat.protocol;

import java.util.Objects;

/**
 * Client unregister request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnregisterRequest extends ClientRequest {
  public static final String NAME = "unregister";

  /**
   * Returns a new unregister request builder.
   *
   * @return A new unregister request builder.
   */
  public static Builder builder() {
    return new Builder(new UnregisterRequest());
  }

  /**
   * Returns a unregister request builder for an existing request.
   *
   * @param request The request to build.
   * @return The unregister request builder.
   * @throws NullPointerException if {@code request} is null
   */
  public static Builder builder(UnregisterRequest request) {
    return new Builder(request);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), client);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof UnregisterRequest) {
      UnregisterRequest request = (UnregisterRequest) object;
      return request.client == client;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[client=%s]", getClass().getSimpleName(), client);
  }

  /**
   * Unregister request builder.
   */
  public static class Builder extends ClientRequest.Builder<Builder, UnregisterRequest> {
    protected Builder(UnregisterRequest request) {
      super(request);
    }
  }

}
