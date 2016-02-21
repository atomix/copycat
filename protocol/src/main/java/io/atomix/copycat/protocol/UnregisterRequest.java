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
 * Session unregister request.
 * <p>
 * The unregister request is sent by a client with an open session to the cluster to explicitly
 * unregister its session. Note that if a client does not send an unregister request, its session will
 * eventually expire. The unregister request simply provides a more orderly method for closing client sessions.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnregisterRequest extends SessionRequest {

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
