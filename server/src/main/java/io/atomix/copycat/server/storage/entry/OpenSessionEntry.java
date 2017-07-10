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
 * Open session entry.
 */
public class OpenSessionEntry extends ClientEntry<OpenSessionEntry> {
  private String name;
  private String type;

  public OpenSessionEntry() {
  }

  public OpenSessionEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the session state machine name.
   *
   * @return The session's state machine name.
   */
  public String getName() {
    return name;
  }

  /**
   * Sets the session's state machine name.
   *
   * @param name The session's state machine name.
   * @return The open session entry.
   */
  public OpenSessionEntry setName(String name) {
    this.name = name;
    return this;
  }

  /**
   * Returns the session state machine type.
   *
   * @return The session's state machine type.
   */
  public String getType() {
    return type;
  }

  /**
   * Sets the session's state machine type.
   *
   * @param type The session's state machine type.
   * @return The open session entry.
   */
  public OpenSessionEntry setType(String type) {
    this.type = type;
    return this;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeString(name);
    buffer.writeString(type);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    name = buffer.readString();
    type = buffer.readString();
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, client=%s, name=%s, type=%s, timestamp=%d]", getClass().getSimpleName(), getIndex(), getTerm(), getClient(), getName(), getType(), getTimestamp());
  }

}
