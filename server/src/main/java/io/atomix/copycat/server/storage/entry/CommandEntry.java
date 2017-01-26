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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.Command;
import io.atomix.copycat.Operation;
import io.atomix.copycat.util.Assert;

/**
 * Stores a state machine {@link Command}.
 * <p>
 * The {@code CommandEntry} is used to store an individual state machine command from an individual
 * client along with information relevant to sequencing the command in the server state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CommandEntry extends OperationEntry<CommandEntry> {
  private final Command command;

  public CommandEntry(long timestamp, long session, long sequence, Command command) {
    super(timestamp, session, sequence);
    this.command = Assert.notNull(command, "command");
  }

  @Override
  public Type<CommandEntry> type() {
    return Type.COMMAND;
  }

  @Override
  public Operation operation() {
    return command;
  }

  /**
   * Returns the command.
   *
   * @return The command.
   */
  public Command command() {
    return command;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, sequence=%d, timestamp=%d, command=%s]", getClass().getSimpleName(), session(), sequence(), timestamp(), command);
  }

  /**
   * Command entry serializer.
   */
  public static class Serializer extends OperationEntry.Serializer<CommandEntry> {
    @Override
    public void write(Kryo kryo, Output output, CommandEntry entry) {
      output.writeLong(entry.timestamp);
      output.writeLong(entry.session);
      output.writeLong(entry.sequence);
      kryo.writeClassAndObject(output, entry.command);
    }

    @Override
    public CommandEntry read(Kryo kryo, Input input, Class<CommandEntry> type) {
      return new CommandEntry(input.readLong(), input.readLong(), input.readLong(), (Command) kryo.readClassAndObject(input));
    }
  }
}
