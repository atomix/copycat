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
 * Command entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=1000)
public class TestEntry extends Entry<TestEntry> {
  private long term;
  private boolean tombstone;

  public TestEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
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

  /**
   * Returns the entry term.
   *
   * @return The entry term.
   */
  public long getTerm() {
    return term;
  }

  /**
   * Sets the entry term.
   *
   * @param term The entry term.
   * @return The entry.
   */
  @SuppressWarnings("unchecked")
  public TestEntry setTerm(long term) {
    this.term = term;
    return this;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeLong(term).writeBoolean(tombstone);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    term = buffer.readLong();
    tombstone = buffer.readBoolean();
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, tombstone=%b]", getClass().getSimpleName(), getIndex(), term, tombstone);
  }

}
