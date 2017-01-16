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
package io.atomix.copycat.protocol.websocket.request;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.request.SessionRequest;

/**
 * Base session request.
 * <p>
 * This is the base request for session-related requests. Many client requests are handled within the
 * context of a {@link #session()} identifier.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class WebSocketSessionRequest extends AbstractWebSocketRequest implements SessionRequest {
  @JsonProperty("session")
  protected final long session;

  protected WebSocketSessionRequest(long id, long session) {
    super(id);
    this.session = session;
  }

  @Override
  @JsonGetter("session")
  public long session() {
    return session;
  }

  /**
   * Session request builder.
   */
  public static abstract class Builder<T extends SessionRequest.Builder<T, U>, U extends SessionRequest> extends AbstractWebSocketRequest.Builder<T, U> implements SessionRequest.Builder<T, U> {
    protected long session;

    protected Builder(long id) {
      super(id);
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withSession(long session) {
      this.session = Assert.argNot(session, session < 1, "session cannot be less than 1");
      return (T) this;
    }
  }
}
