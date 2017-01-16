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
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Command;
import io.atomix.copycat.protocol.request.CommandRequest;

import java.util.Objects;

/**
 * Client command request.
 * <p>
 * Command requests are submitted by clients to the Copycat cluster to commit {@link Command}s to
 * the replicated state machine. Each command request must be associated with a registered
 * {@link #session()} and have a unique {@link #sequence()} number within that session. Commands will
 * be applied in the cluster in the order defined by the provided sequence number. Thus, sequence numbers
 * should never be skipped. In the event of a failure of a command request, the request should be resent
 * with the same sequence number. Commands are guaranteed to be applied in sequence order.
 * <p>
 * Command requests should always be submitted to the server to which the client is connected and will
 * be forwarded to the current cluster leader. In the event that no leader is available, the request
 * will fail and should be resubmitted by the client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class WebSocketCommandRequest extends WebSocketOperationRequest implements CommandRequest {
  @JsonProperty("command")
  private Command command;

  @JsonCreator
  protected WebSocketCommandRequest(@JsonProperty("id") long id, @JsonProperty("session") long session, @JsonProperty("sequence") long sequence, @JsonProperty("command") Command command) {
    super(id, session, sequence);
    this.command = command;
  }

  @Override
  @JsonGetter("type")
  public Type type() {
    return Type.COMMAND_REQUEST;
  }

  @Override
  @JsonGetter("command")
  public Command command() {
    return command;
  }

  @Override
  public Command operation() {
    return command;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, sequence, command);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof WebSocketCommandRequest) {
      WebSocketCommandRequest request = (WebSocketCommandRequest) object;
      return request.session == session
        && request.sequence == sequence
        && request.command.equals(command);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, sequence=%d, command=%s]", getClass().getSimpleName(), session, sequence, command);
  }

  /**
   * Write request builder.
   */
  public static class Builder extends WebSocketOperationRequest.Builder<CommandRequest.Builder, CommandRequest> implements CommandRequest.Builder {
    private Command command;

    public Builder(long id) {
      super(id);
    }

    @Override
    public Builder withCommand(Command command) {
      this.command = Assert.notNull(command, "command");
      return this;
    }

    /**
     * @throws IllegalStateException if session or sequence are less than 1, or command is null
     */
    @Override
    public CommandRequest build() {
      return new WebSocketCommandRequest(id, session, sequence, command);
    }
  }
}
