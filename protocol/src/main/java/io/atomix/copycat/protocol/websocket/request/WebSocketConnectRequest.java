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
import io.atomix.copycat.protocol.request.ConnectRequest;

/**
 * Web socket connect request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class WebSocketConnectRequest extends ConnectRequest implements WebSocketRequest {
  private final long id;

  @JsonCreator
  public WebSocketConnectRequest(
    @JsonProperty("id") long id,
    @JsonProperty("client") String client) {
    super(client);
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
    return Types.CONNECT_REQUEST;
  }

  @Override
  @JsonGetter("client")
  public String client() {
    return super.client();
  }

  /**
   * Web socket connect request builder.
   */
  public static class Builder extends ConnectRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public ConnectRequest copy(ConnectRequest request) {
      return new WebSocketConnectRequest(id, request.client());
    }

    @Override
    public ConnectRequest build() {
      return new WebSocketConnectRequest(id, client);
    }
  }
}
