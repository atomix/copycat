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
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.response.KeepAliveResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Web socket keep alive response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class WebSocketKeepAliveResponse extends KeepAliveResponse implements WebSocketResponse<WebSocketKeepAliveResponse> {
  private final long id;

  @JsonCreator
  protected WebSocketKeepAliveResponse(
    @JsonProperty("id") long id,
    @JsonProperty("status") Status status,
    @JsonProperty("error") WebSocketResponse.Error error,
    @JsonProperty("leader") String leader,
    @JsonProperty("members") Collection<String> members) {
    this(id, status, error, leader != null ? new Address(leader) : null, members != null ? members.stream().map(Address::new).collect(Collectors.toList()) : null);
  }

  public WebSocketKeepAliveResponse(long id, Status status, WebSocketResponse.Error error, Address leader, Collection<Address> members) {
    super(status, error, leader, members);
    this.id = id;
  }

  @Override
  @JsonGetter("id")
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.KEEP_ALIVE;
  }

  /**
   * Returns the response type name.
   *
   * @return The response type name.
   */
  @JsonGetter("type")
  private String typeName() {
    return type().name();
  }

  @Override
  @JsonGetter("status")
  public Status status() {
    return super.status();
  }

  @Override
  @JsonGetter("error")
  public WebSocketResponse.Error error() {
    return (WebSocketResponse.Error) super.error();
  }

  @Override
  public Address leader() {
    return super.leader();
  }

  @JsonGetter("leader")
  private String leaderString() {
    return leader != null ? leader.toString() : null;
  }

  @Override
  public Collection<Address> members() {
    return super.members();
  }

  @JsonGetter("members")
  private Collection<String> memberStrings() {
    return members != null ? members().stream().map(Address::toString).collect(Collectors.toList()) : null;
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
    public KeepAliveResponse.Builder withError(ProtocolResponse.Error.Type type, String message) {
      this.error = new WebSocketResponse.Error(type, message);
      return this;
    }

    @Override
    public KeepAliveResponse copy(KeepAliveResponse response) {
      final WebSocketResponse.Error error = response.error() != null ? new WebSocketResponse.Error(response.error().type(), response.error().message()) : null;
      return new WebSocketKeepAliveResponse(id, response.status(), error, response.leader(), response.members());
    }

    @Override
    public KeepAliveResponse build() {
      return new WebSocketKeepAliveResponse(id, status, (WebSocketResponse.Error) error, leader, members);
    }
  }
}
