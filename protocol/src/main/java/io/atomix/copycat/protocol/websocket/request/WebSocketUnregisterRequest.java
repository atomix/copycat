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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.atomix.copycat.protocol.request.UnregisterRequest;

/**
 * Web socket unregister request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class WebSocketUnregisterRequest extends UnregisterRequest implements WebSocketRequest<WebSocketUnregisterRequest> {
  private final long id;

  @JsonCreator
  public WebSocketUnregisterRequest(
    @JsonProperty("id") long id,
    @JsonProperty("session") long session) {
    super(session);
    this.id = id;
  }

  @Override
  @JsonGetter("id")
  public long id() {
    return id;
  }

  @Override
  @JsonGetter("type")
  public Type type() {
    return Type.UNREGISTER;
  }

  @Override
  @JsonGetter("session")
  public long session() {
    return super.session();
  }

  /**
   * Web socket unregister request builder.
   */
  public static class Builder extends UnregisterRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public UnregisterRequest copy(UnregisterRequest request) {
      return new WebSocketUnregisterRequest(id, request.session());
    }

    @Override
    public UnregisterRequest build() {
      return new WebSocketUnregisterRequest(id, session);
    }
  }
}
