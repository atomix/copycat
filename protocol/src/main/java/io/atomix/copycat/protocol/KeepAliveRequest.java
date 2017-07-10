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
package io.atomix.copycat.protocol;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;

import java.util.Arrays;
import java.util.Objects;

/**
 * Session keep alive request.
 * <p>
 * Keep alive requests are sent by clients to servers to maintain a session registered via
 * a {@link RegisterRequest}. Once a session has been registered, clients are responsible for sending
 * keep alive requests to the cluster at a rate less than the provided {@link RegisterResponse#timeout()}.
 * Keep alive requests also server to acknowledge the receipt of responses and events by the client.
 * The {@link #commandSequences()} number indicates the highest command sequence number for which the client
 * has received a response, and the {@link #eventIndexes()} numbers indicate the highest index for which the
 * client has received an event in proper sequence.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class KeepAliveRequest extends ClientRequest {
  public static final String NAME = "keep-alive";

  /**
   * Returns a new keep alive request builder.
   *
   * @return A new keep alive request builder.
   */
  public static Builder builder() {
    return new Builder(new KeepAliveRequest());
  }

  /**
   * Returns a keep alive request builder for an existing request.
   *
   * @param request The request to build.
   * @return The keep alive request builder.
   * @throws NullPointerException if {@code request} is null
   */
  public static Builder builder(KeepAliveRequest request) {
    return new Builder(request);
  }

  private long[] sessionIds;
  private long[] commandSequences;
  private long[] eventIndexes;
  private long[] connections;

  /**
   * Returns the session identifiers.
   *
   * @return The session identifiers.
   */
  public long[] sessionIds() {
    return sessionIds;
  }

  /**
   * Returns the command sequence numbers.
   *
   * @return The command sequence numbers.
   */
  public long[] commandSequences() {
    return commandSequences;
  }

  /**
   * Returns the event indexes.
   *
   * @return The event indexes.
   */
  public long[] eventIndexes() {
    return eventIndexes;
  }

  /**
   * Returns the session connections.
   *
   * @return The session connections.
   */
  public long[] connections() {
    return connections;
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
  public int hashCode() {
    return Objects.hash(getClass(), client, commandSequences, eventIndexes);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof KeepAliveRequest) {
      KeepAliveRequest request = (KeepAliveRequest) object;
      return request.client == client
        && Arrays.equals(request.commandSequences, commandSequences)
        && Arrays.equals(request.eventIndexes, eventIndexes);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[client=%s, sessionIds=%s, commandSequences=%s, eventIndexes=%s, connections=%s]", getClass().getSimpleName(), client, Arrays.toString(sessionIds), Arrays.toString(commandSequences), Arrays.toString(eventIndexes), Arrays.toString(connections));
  }

  /**
   * Keep alive request builder.
   */
  public static class Builder extends ClientRequest.Builder<Builder, KeepAliveRequest> {
    protected Builder(KeepAliveRequest request) {
      super(request);
    }

    /**
     * Sets the session identifiers.
     *
     * @param sessionIds The session identifiers.
     * @return The request builders.
     * @throws NullPointerException if {@code sessionIds} is {@code null}
     */
    public Builder withSessionIds(long[] sessionIds) {
      request.sessionIds = Assert.notNull(sessionIds, "sessionIds");
      return this;
    }

    /**
     * Sets the command sequence numbers.
     *
     * @param commandSequences The command sequence numbers.
     * @return The request builder.
     * @throws NullPointerException if {@code commandSequences} is {@code null}
     */
    public Builder withCommandSequences(long[] commandSequences) {
      request.commandSequences = Assert.notNull(commandSequences, "commandSequences");
      return this;
    }

    /**
     * Sets the event indexes.
     *
     * @param eventIndexes The event indexes.
     * @return The request builder.
     * @throws NullPointerException if {@code eventIndexes} is {@code null}
     */
    public Builder withEventIndexes(long[] eventIndexes) {
      request.eventIndexes = Assert.notNull(eventIndexes, "eventIndexes");
      return this;
    }

    /**
     * Sets the client connections.
     *
     * @param connections The client connections.
     * @return The request builder.
     * @throws NullPointerException if {@code connections} is {@code null}
     */
    public Builder withConnections(long[] connections) {
      request.connections = Assert.notNull(connections, "connections");
      return this;
    }

    @Override
    public KeepAliveRequest build() {
      super.build();
      Assert.notNull(request.sessionIds, "sessionIds");
      Assert.notNull(request.commandSequences, "commandSequences");
      Assert.notNull(request.eventIndexes, "eventIndexes");
      Assert.notNull(request.connections, "connections");
      return request;
    }
  }

}
