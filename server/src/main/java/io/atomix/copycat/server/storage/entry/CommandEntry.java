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
package io.atomix.copycat.server.storage.entry;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ReferenceManager;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.Operation;

/**
 * Command entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=221)
public class CommandEntry extends OperationEntry<CommandEntry> {
  private Command command;

  public CommandEntry() {
  }

  public CommandEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  @Override
  public boolean isTombstone() {
    return command.compact() == Command.CompactionMode.SEQUENTIAL;
  }

  @Override
  public boolean isSnapshotted() {
    return command.compact() == Command.CompactionMode.SNAPSHOT;
  }

  @Override
  public Operation getOperation() {
    return command;
  }

  /**
   * Returns the command.
   *
   * @return The command.
   */
  public Command getCommand() {
    return command;
  }

  /**
   * Sets the command.
   *
   * @param command The command.
   * @return The command entry.
   * @throws NullPointerException if {@code command} is null
   */
  public CommandEntry setCommand(Command command) {
    this.command = Assert.notNull(command, "command");
    return this;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    serializer.writeObject(command, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    command = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, session=%d, sequence=%d, timestamp=%d, command=%s]", getClass().getSimpleName(), getIndex(), getTerm(), getSession(), getSequence(), getTimestamp(), command);
  }

}
