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
import io.atomix.catalyst.util.ReferenceManager;
import io.atomix.copycat.server.storage.compaction.Compaction;

/**
 * Unregister entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=228)
public class UnregisterEntry extends SessionEntry<UnregisterEntry> {
  private boolean expired;

  public UnregisterEntry() {
  }

  public UnregisterEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Compaction.Mode getCompactionMode() {
    return Compaction.Mode.SEQUENTIAL;
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
