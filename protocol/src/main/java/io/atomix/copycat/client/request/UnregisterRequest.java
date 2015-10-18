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
package io.atomix.copycat.client.request;

import io.atomix.catalyst.serializer.SerializeWith;

import java.util.Objects;

/**
 * Protocol unregister request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=213)
public class UnregisterRequest extends SessionRequest<UnregisterRequest> {

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
    return Objects.hash(getClass(), session);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof UnregisterRequest) {
      UnregisterRequest request = (UnregisterRequest) object;
      return request.session == session;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d]", getClass().getSimpleName(), session);
  }

  /**
   * Unregister request builder.
   */
  public static class Builder extends SessionRequest.Builder<Builder, UnregisterRequest> {
    protected Builder(UnregisterRequest request) {
      super(request);
    }
  }

}
