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
import io.atomix.copycat.protocol.UnregisterRequest;
import io.atomix.copycat.server.storage.compaction.Compaction;

/**
 * Stores a client session close.
 * <p>
 * The {@code UnregisterEntry} is stored in reaction to receiving a
 * {@link UnregisterRequest} from a client or in reaction to a leader
 * expiring a session on the server. When a leader expires a session, the leader must commit an
 * {@code UnregisterEntry} to ensure that the session is expired deterministically across the cluster.
 * The {@link #isExpired()} method indicates whether the entry represents a session being expired by
 * the leader or an explicit request by a client to close its session.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class UnregisterEntry extends SessionEntry<UnregisterEntry> {
  private boolean expired;

  public UnregisterEntry() {
  }

  public UnregisterEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Compaction.Mode getCompactionMode() {
    return Compaction.Mode.TOMBSTONE;
  }

  /**
   * Sets whether the session was expired by a leader.
   *
   * @param expired Whether the session was expired by a leader.
   * @return The unregister entry.
   */
  public UnregisterEntry setExpired(boolean expired) {
    this.expired = expired;
    return this;
  }

  /**
   * Returns whether the session was expired by a leader.
   *
   * @return Whether the session was expired by a leader.
   */
  public boolean isExpired() {
    return expired;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeBoolean(expired);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    expired = buffer.readBoolean();
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, session=%d, expired=%b, timestamp=%d]", getClass().getSimpleName(), getIndex(), getTerm(), getSession(), isExpired(), getTimestamp());
  }

}
