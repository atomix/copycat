/*
 * Copyright 2015 the original author or authors.
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
 * limitations under the License.
 */
package io.atomix.copycat.client.request;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Operation;

import java.util.Objects;

/**
 * Protocol command request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=201)
public class CommandRequest extends OperationRequest<CommandRequest> {

  /**
   * Returns a new submit request builder.
   *
   * @return A new submit request builder.
   */
  public static Builder builder() {
    return new Builder(new CommandRequest());
  }

  /**
   * Returns a submit request builder for an existing request.
   *
   * @param request The request to build.
   * @return The submit request builder.
   * @throws NullPointerException if {@code request} is null
   */
  public static Builder builder(CommandRequest request) {
    return new Builder(request);
  }

  private Command command;

  /**
   * Returns the command.
   *
   * @return The command.
   */
  public Command command() {
    return command;
  }

  @Override
  public Operation operation() {
    return command;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    command = serializer.readObject(buffer);
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    serializer.writeObject(command, buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, sequence, command);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof CommandRequest) {
      CommandRequest request = (CommandRequest) object;
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
  public static class Builder extends OperationRequest.Builder<Builder, CommandRequest> {
    protected Builder(CommandRequest request) {
      super(request);
    }

    /**
     * Sets the request command.
     *
     * @param command The request command.
     * @return The request builder.
     * @throws NullPointerException if {@code command} is null
     */
    public Builder withCommand(Command command) {
      request.command = Assert.notNull(command, "command");
      return this;
    }

    /**
     * @throws IllegalStateException if session or sequence are less than 1, or command is null
     */
    @Override
    public CommandRequest build() {
      super.build();
      Assert.stateNot(request.command == null, "command cannot be null");
      return request;
    }
  }

}
