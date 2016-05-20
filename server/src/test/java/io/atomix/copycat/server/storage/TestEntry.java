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
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.reference.ReferenceManager;
import io.atomix.copycat.server.storage.compaction.Compaction;
import io.atomix.copycat.server.storage.entry.Entry;

/**
 * Test entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestEntry extends Entry<TestEntry> {
  /** Padding to vary the stored size of an entry */
  private Compaction.Mode compaction = Compaction.Mode.QUORUM;
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
    compaction = Compaction.Mode.values()[buffer.readByte()];
    paddingSize = buffer.readInt();
    padding = new byte[paddingSize];
    buffer.read(padding);
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(getTerm()).writeByte(compaction.ordinal()).writeInt(paddingSize).write(padding);
  }

  public byte[] getPadding() {
    return padding;
  }

  public void setPadding(int paddingSize) {
    this.paddingSize = paddingSize;
    this.padding = new byte[paddingSize];
  }

  @Override
  public Compaction.Mode getCompactionMode() {
    return compaction;
  }

  public TestEntry setCompactionMode(Compaction.Mode mode) {
    this.compaction = mode;
    return this;
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, compaction=%s]", getClass().getSimpleName(), getIndex(), getTerm(), compaction);
  }

}
