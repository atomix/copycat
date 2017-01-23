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
import io.atomix.copycat.server.storage.compaction.Compaction;

/**
 * Stores a client session close.
 * <p>
 * The {@code UnregisterEntry} is stored in reaction to receiving a
 * {@link io.atomix.copycat.protocol.request.UnregisterRequest} from a client or in reaction to a leader
 * expiring a session on the server. When a leader expires a session, the leader must commit an
 * {@code UnregisterEntry} to ensure that the session is expired deterministically across the cluster.
 * The {@link #expired()} method indicates whether the entry represents a session being expired by
 * the leader or an explicit request by a client to close its session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnregisterEntry extends SessionEntry<UnregisterEntry> {
  private final boolean expired;

  public UnregisterEntry(long timestamp, long session, boolean expired) {
    super(timestamp, session);
    this.expired = expired;
  }

  @Override
  public Type<UnregisterEntry> type() {
    return Type.UNREGISTER;
  }

  @Override
  public Compaction.Mode compaction() {
    return Compaction.Mode.TOMBSTONE;
  }

  /**
   * Returns whether the session was expired by a leader.
   *
   * @return Whether the session was expired by a leader.
   */
  public boolean expired() {
    return expired;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, expired=%b, timestamp=%d]", getClass().getSimpleName(), session(), expired(), timestamp());
  }

  /**
   * Unregister entry serializer.
   */
  public static class Serializer extends SessionEntry.Serializer<UnregisterEntry> {
    @Override
    public void write(Kryo kryo, Output output, UnregisterEntry entry) {
      output.writeLong(entry.timestamp);
      output.writeLong(entry.session);
      output.writeBoolean(entry.expired);
    }

    @Override
    public UnregisterEntry read(Kryo kryo, Input input, Class<UnregisterEntry> type) {
      return new UnregisterEntry(input.readLong(), input.readLong(), input.readBoolean());
    }
  }
}
