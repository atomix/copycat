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
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.reference.ReferenceManager;
import io.atomix.copycat.protocol.KeepAliveRequest;
import io.atomix.copycat.session.Session;

/**
 * Stores a client keep-alive request.
 * <p>
 * The {@code KeepAliveEntry} is logged and replicated to the cluster to indicate that a client
 * has kept its {@link #getSession() session} alive. Each client must periodically submit a
 * {@link KeepAliveRequest} which results in a keep-alive entry
 * being written to the Raft log. When a keep-alive is committed to the internal Raft state machine,
 * the session timeout for the associated {@link Session} will be
 * reset.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KeepAliveEntry extends SessionEntry<KeepAliveEntry> {
  private long commandSequence;
  private long eventIndex;

  public KeepAliveEntry() {
  }

  public KeepAliveEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the command sequence number.
   *
   * @return The command sequence number.
   */
  public long getCommandSequence() {
    return commandSequence;
  }

  /**
   * Sets the command sequence number.
   *
   * @param commandSequence The command sequence number.
   * @return The keep alive entry.
   */
  public KeepAliveEntry setCommandSequence(long commandSequence) {
    this.commandSequence = commandSequence;
    return this;
  }

  /**
   * Returns the event index.
   *
   * @return The event index.
   */
  public long getEventIndex() {
    return eventIndex;
  }

  /**
   * Sets the event index.
   *
   * @param eventIndex The event index.
   * @return The keep alive entry.
   */
  public KeepAliveEntry setEventIndex(long eventIndex) {
    this.eventIndex = eventIndex;
    return this;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    commandSequence = buffer.readLong();
    eventIndex = buffer.readLong();
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(commandSequence);
    buffer.writeLong(eventIndex);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, session=%d, commandSequence=%d, eventIndex=%d, timestamp=%d]", getClass().getSimpleName(), getIndex(), getTerm(), getSession(), getCommandSequence(), getEventIndex(), getTimestamp());
  }

}
