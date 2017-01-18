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
package io.atomix.copycat.protocol.local.request;

import io.atomix.copycat.protocol.request.UnregisterRequest;

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
public class LocalUnregisterRequest extends LocalSessionRequest implements UnregisterRequest {
  protected LocalUnregisterRequest(long session) {
    super(session);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LocalUnregisterRequest) {
      LocalUnregisterRequest request = (LocalUnregisterRequest) object;
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
  public static class Builder extends LocalSessionRequest.Builder<UnregisterRequest.Builder, UnregisterRequest> implements UnregisterRequest.Builder {
    @Override
    public UnregisterRequest build() {
      return new LocalUnregisterRequest(session);
    }
  }
}
