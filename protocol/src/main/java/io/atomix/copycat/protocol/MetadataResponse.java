/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.metadata.CopycatSessionMetadata;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Cluster metadata response.
 */
public class MetadataResponse extends AbstractResponse {

  /**
   * Returns a new metadata response builder.
   *
   * @return A new metadata response builder.
   */
  public static Builder builder() {
    return new Builder(new MetadataResponse());
  }

  /**
   * Returns a new metadata response builder.
   *
   * @param request The metadata response to build.
   * @return A new metadata response builder.
   */
  public static Builder builder(MetadataResponse request) {
    return new Builder(request);
  }

  private Set<CopycatSessionMetadata> sessions;

  /**
   * Returns the session metadata.
   *
   * @return Session metadata.
   */
  public Set<CopycatSessionMetadata> sessions() {
    return sessions;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      int sessionCount = buffer.readInt();
      sessions = new HashSet<>();
      for (int i = 0; i < sessionCount; i++) {
        sessions.add(new CopycatSessionMetadata(buffer.readLong(), buffer.readString(), buffer.readString()));
      }
    } else {
      error = CopycatError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.ERROR) {
      buffer.writeByte(error.id());
    } else {
      buffer.writeInt(sessions.size());
      for (CopycatSessionMetadata session : sessions) {
        buffer.writeLong(session.id());
        buffer.writeString(session.name());
        buffer.writeString(session.type());
      }
    }
  }

  /**
   * Metadata response builder.
   */
  public static class Builder extends AbstractResponse.Builder<Builder, MetadataResponse> {
    public Builder(MetadataResponse request) {
      super(request);
    }

    /**
     * Sets the session metadata.
     *
     * @param sessions The client metadata.
     * @return The metadata response builder.
     */
    public Builder withSessions(CopycatSessionMetadata... sessions) {
      return withSessions(Arrays.asList(Assert.notNull(sessions, "sessions")));
    }

    /**
     * Sets the session metadata.
     *
     * @param sessions The client metadata.
     * @return The metadata response builder.
     */
    public Builder withSessions(Collection<CopycatSessionMetadata> sessions) {
      response.sessions = new HashSet<>(Assert.notNull(sessions, "sessions"));
      return this;
    }

    @Override
    public MetadataResponse build() {
      super.build();
      Assert.notNull(response.sessions, "sessions");
      return response;
    }
  }
}
