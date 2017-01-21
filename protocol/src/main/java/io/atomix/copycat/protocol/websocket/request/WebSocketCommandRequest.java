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
import io.atomix.copycat.Command;
import io.atomix.copycat.protocol.request.CommandRequest;

/**
 * Web socket command request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class WebSocketCommandRequest extends CommandRequest implements WebSocketRequest<WebSocketCommandRequest> {
  private final long id;

  @JsonCreator
  public WebSocketCommandRequest(
    @JsonProperty("id") long id,
    @JsonProperty("session") long session,
    @JsonProperty("sequence") long sequence,
    @JsonProperty("command") Command command) {
    super(session, sequence, command);
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
    return Type.COMMAND;
  }

  @Override
  @JsonGetter("session")
  public long session() {
    return super.session();
  }

  @Override
  @JsonGetter("sequence")
  public long sequence() {
    return super.sequence();
  }

  @Override
  @JsonGetter("command")
  public Command command() {
    return super.command();
  }

  /**
   * Web socket command request builder.
   */
  public static class Builder extends CommandRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public CommandRequest copy(CommandRequest request) {
      return new WebSocketCommandRequest(id, request.session(), request.sequence(), request.command());
    }

    @Override
    public CommandRequest build() {
      return new WebSocketCommandRequest(id, session, sequence, command);
    }
  }
}
