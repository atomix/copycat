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
import io.atomix.copycat.protocol.response.RegisterResponse;

import java.util.Collection;

/**
 * Web socket register response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class WebSocketRegisterResponse extends RegisterResponse implements WebSocketResponse<WebSocketRegisterResponse> {
  private final long id;

  @JsonCreator
  public WebSocketRegisterResponse(
    @JsonProperty("id") long id,
    @JsonProperty("status") Status status,
    @JsonProperty("error") CopycatError error,
    @JsonProperty("session") long session,
    @JsonProperty("leader") Address leader,
    @JsonProperty("members") Collection<Address> members,
    @JsonProperty("timeout") long timeout) {
    super(status, error, session, leader, members, timeout);
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
    return Type.REGISTER;
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
  @JsonGetter("session")
  public long session() {
    return super.session();
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

  @Override
  @JsonGetter("timeout")
  public long timeout() {
    return super.timeout();
  }

  /**
   * Web socket register response builder.
   */
  public static class Builder extends RegisterResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public RegisterResponse copy(RegisterResponse response) {
      return new WebSocketRegisterResponse(id, response.status(), response.error(), response.session(), response.leader(), response.members(), response.timeout());
    }

    @Override
    public RegisterResponse build() {
      return new WebSocketRegisterResponse(id, status, error, session, leader, members, timeout);
    }
  }
}
