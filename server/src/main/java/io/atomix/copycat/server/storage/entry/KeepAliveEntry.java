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

import java.util.Arrays;

/**
 * Stores a client keep-alive request.
 * <p>
 * The {@code KeepAliveEntry} is logged and replicated to the cluster to indicate that a client
 * has kept its {@link #getClient() client} alive. Each client must periodically submit a
 * {@link KeepAliveRequest} which results in a keep-alive entry
 * being written to the Raft log. When a keep-alive is committed to the internal Raft state machine,
 * the session timeout for the associated {@link Session} will be
 * reset.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KeepAliveEntry extends ClientEntry<KeepAliveEntry> {
  private long[] sessionIds;
  private long[] commandSequences;
  private long[] eventIndexes;
  private long[] connections;

  public KeepAliveEntry() {
  }

  public KeepAliveEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the session identifiers.
   *
   * @return The session identifiers.
   */
  public long[] getSessionIds() {
    return sessionIds;
  }

  /**
   * Sets the session identifiers.
   *
   * @param sessionIds The session identifiers.
   * @return The keep alive entry.
   */
  public KeepAliveEntry setSessionIds(long[] sessionIds) {
    this.sessionIds = sessionIds;
    return this;
  }

  /**
   * Returns the command sequence numbers.
   *
   * @return The command sequence numbers.
   */
  public long[] getCommandSequences() {
    return commandSequences;
  }

  /**
   * Sets the command sequence numbers.
   *
   * @param commandSequence The command sequence numbers.
   * @return The keep alive entry.
   */
  public KeepAliveEntry setCommandSequences(long[] commandSequence) {
    this.commandSequences = commandSequence;
    return this;
  }

  /**
   * Returns the event indexes.
   *
   * @return The event indexes.
   */
  public long[] getEventIndexes() {
    return eventIndexes;
  }

  /**
   * Sets the event indexes.
   *
   * @param eventIndexes The event indexes.
   * @return The keep alive entry.
   */
  public KeepAliveEntry setEventIndexes(long[] eventIndexes) {
    this.eventIndexes = eventIndexes;
    return this;
  }

  /**
   * Returns the connection IDs.
   *
   * @return The connection IDs.
   */
  public long[] getConnections() {
    return connections;
  }

  /**
   * Sets the connection IDs.
   *
   * @param connections The connection IDs.
   * @return The keep alive entry.
   */
  public KeepAliveEntry setConnections(long[] connections) {
    this.connections = connections;
    return this;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    int sessionsLength = buffer.readInt();
    sessionIds = new long[sessionsLength];
    for (int i = 0; i < sessionsLength; i++) {
      sessionIds[i] = buffer.readLong();
    }

    int commandSequencesLength = buffer.readInt();
    commandSequences = new long[commandSequencesLength];
    for (int i = 0; i < commandSequencesLength; i++) {
      commandSequences[i] = buffer.readLong();
    }

    int eventIndexesLength = buffer.readInt();
    eventIndexes = new long[eventIndexesLength];
    for (int i = 0; i < eventIndexesLength; i++) {
      eventIndexes[i] = buffer.readLong();
    }

    int connectionsLength = buffer.readInt();
    connections = new long[connectionsLength];
    for (int i = 0; i < connectionsLength; i++) {
      connections[i] = buffer.readLong();
    }
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeInt(sessionIds.length);
    for (long sessionId : sessionIds) {
      buffer.writeLong(sessionId);
    }

    buffer.writeInt(commandSequences.length);
    for (long commandSequence : commandSequences) {
      buffer.writeLong(commandSequence);
    }

    buffer.writeInt(eventIndexes.length);
    for (long eventIndex : eventIndexes) {
      buffer.writeLong(eventIndex);
    }

    buffer.writeInt(connections.length);
    for (long connection : connections) {
      buffer.writeLong(connection);
    }
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, client=%s, sessions=%s, commandSequences=%s, eventIndexes=%s, connections=%s, timestamp=%d]", getClass().getSimpleName(), getIndex(), getTerm(), getClient(), Arrays.toString(getSessionIds()), Arrays.toString(getCommandSequences()), Arrays.toString(getEventIndexes()), Arrays.toString(getConnections()), getTimestamp());
  }

}
