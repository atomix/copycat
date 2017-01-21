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
package io.atomix.copycat.protocol.websocket.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.response.KeepAliveResponse;

import java.util.Collection;

/**
 * Web socket keep alive response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class WebSocketKeepAliveResponse extends KeepAliveResponse implements WebSocketResponse<WebSocketKeepAliveResponse> {
  private final long id;

  @JsonCreator
  public WebSocketKeepAliveResponse(
    @JsonProperty("id") long id,
    @JsonProperty("status") Status status,
    @JsonProperty("error") CopycatError error,
    @JsonProperty("leader") Address leader,
    @JsonProperty("members") Collection<Address> members) {
    super(status, error, leader, members);
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
    return Type.KEEP_ALIVE;
  }

  @Override
  @JsonGetter("status")
  public Status status() {
    return super.status();
  }

  @Override
  @JsonGetter("error")
  public CopycatError error() {
    return super.error();
  }

  @Override
  @JsonGetter("leader")
  public Address leader() {
    return super.leader();
  }

  @Override
  @JsonGetter("members")
  public Collection<Address> members() {
    return super.members();
  }

  /**
   * Web socket keep alive response builder.
   */
  public static class Builder extends KeepAliveResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public KeepAliveResponse copy(KeepAliveResponse response) {
      return new WebSocketKeepAliveResponse(id, response.status(), response.error(), response.leader(), response.members());
    }

    @Override
    public KeepAliveResponse build() {
      return new WebSocketKeepAliveResponse(id, status, error, leader, members);
    }
  }
}
