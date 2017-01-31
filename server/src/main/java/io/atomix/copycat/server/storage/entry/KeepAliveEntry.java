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

import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

/**
 * Stores a client keep-alive request.
 * <p>
 * The {@code KeepAliveEntry} is logged and replicated to the cluster to indicate that a client
 * has kept its {@link #session() session} alive. Each client must periodically submit a
 * {@link io.atomix.copycat.protocol.request.KeepAliveRequest} which results in a keep-alive entry
 * being written to the Raft log. When a keep-alive is committed to the internal Raft state machine,
 * the session timeout for the associated session will be reset.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KeepAliveEntry extends SessionEntry<KeepAliveEntry> {
  private final long commandSequence;
  private final long eventIndex;

  public KeepAliveEntry(long timestamp, long session, long commandSequence, long eventIndex) {
    super(timestamp, session);
    this.commandSequence = commandSequence;
    this.eventIndex = eventIndex;
  }

  @Override
  public Type<KeepAliveEntry> type() {
    return Type.KEEP_ALIVE;
  }

  /**
   * Returns the command sequence number.
   *
   * @return The command sequence number.
   */
  public long commandSequence() {
    return commandSequence;
  }

  /**
   * Returns the event index.
   *
   * @return The event index.
   */
  public long eventIndex() {
    return eventIndex;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, commandSequence=%d, eventIndex=%d, timestamp=%d]", getClass().getSimpleName(), session(), commandSequence(), eventIndex(), timestamp());
  }

  /**
   * Keep alive entry serializer.
   */
  public static class Serializer implements SessionEntry.Serializer<KeepAliveEntry> {
    @Override
    public void writeObject(BufferOutput output, KeepAliveEntry entry) {
      output.writeLong(entry.timestamp);
      output.writeLong(entry.session);
      output.writeLong(entry.commandSequence);
      output.writeLong(entry.eventIndex);
    }

    @Override
    public KeepAliveEntry readObject(BufferInput input, Class<KeepAliveEntry> type) {
      return new KeepAliveEntry(input.readLong(), input.readLong(), input.readLong(), input.readLong());
    }
  }
}
