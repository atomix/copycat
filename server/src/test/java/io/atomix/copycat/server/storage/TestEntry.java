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
package io.atomix.copycat.server.storage;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.ReferenceManager;
import io.atomix.copycat.server.storage.entry.Entry;

/**
 * Test entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id = 1000)
public class TestEntry extends Entry<TestEntry> {
  private boolean tombstone;
  /** Padding to vary the stored size of an entry */
  private int paddingSize;
  private byte[] padding = new byte[0];

  public TestEntry() {
  }

  public TestEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    setTerm(buffer.readLong());
    tombstone = buffer.readBoolean();
    paddingSize = buffer.readInt();
    padding = new byte[paddingSize];
    buffer.read(padding);
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(getTerm()).writeBoolean(tombstone).writeInt(paddingSize).write(padding);
  }

  public byte[] getPadding() {
    return padding;
  }

  public void setPadding(int paddingSize) {
    this.paddingSize = paddingSize;
    this.padding = new byte[paddingSize];
  }

  @Override
  public boolean isTombstone() {
    return tombstone;
  }

  /**
   * Sets whether the entry is a tombstone.
   *
   * @param tombstone Whether the entry is a tombstone.
   * @return The test entry.
   */
  public TestEntry setTombstone(boolean tombstone) {
    this.tombstone = tombstone;
    return this;
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, tombstone=%b]", getClass().getSimpleName(), getIndex(), getTerm(), tombstone);
  }

}
