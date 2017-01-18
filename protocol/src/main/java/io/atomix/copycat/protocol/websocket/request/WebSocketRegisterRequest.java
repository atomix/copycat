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
import io.atomix.copycat.protocol.request.RegisterRequest;

/**
 * Web socket register request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class WebSocketRegisterRequest extends RegisterRequest implements WebSocketRequest {
  private final long id;

  @JsonCreator
  public WebSocketRegisterRequest(
    @JsonProperty("id") long id,
    @JsonProperty("client") String client,
    @JsonProperty("timeout") long timeout) {
    super(client, timeout);
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
    return Types.REGISTER_REQUEST;
  }

  @Override
  @JsonGetter("client")
  public String client() {
    return super.client();
  }

  @Override
  @JsonGetter("timeout")
  public long timeout() {
    return super.timeout();
  }

  /**
   * Web socket register request builder.
   */
  public static class Builder extends RegisterRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public RegisterRequest copy(RegisterRequest request) {
      return new WebSocketRegisterRequest(id, request.client(), request.timeout());
    }

    @Override
    public RegisterRequest build() {
      return new WebSocketRegisterRequest(id, client, timeout);
    }
  }
}
