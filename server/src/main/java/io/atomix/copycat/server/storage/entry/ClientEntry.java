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
package io.atomix.copycat.server.storage.entry;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.reference.ReferenceManager;

/**
 * Base class for client-related entries.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ClientEntry<T extends ClientEntry<T>> extends TimestampedEntry<T> {
  private long client;

  protected ClientEntry() {
  }

  protected ClientEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Sets the client ID.
   *
   * @param client The client ID.
   * @return The client entry.
   */
  @SuppressWarnings("unchecked")
  public T setClient(long client) {
    this.client = client;
    return (T) this;
  }

  /**
   * Returns the client ID.
   *
   * @return The client ID.
   */
  public long getClient() {
    return client;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(client);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    client = buffer.readLong();
  }

}
