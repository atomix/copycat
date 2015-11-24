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
import io.atomix.catalyst.util.ReferenceManager;
import io.atomix.copycat.client.Operation;

/**
 * Operation entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class OperationEntry<T extends OperationEntry<T>> extends SessionEntry<T> {
  private long sequence;

  protected OperationEntry() {
  }

  protected OperationEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the entry operation.
   *
   * @return The entry operation.
   */
  public abstract Operation<T> getOperation();

  /**
   * Returns the operation sequence number.
   *
   * @return The operation sequence number.
   */
  public long getSequence() {
    return sequence;
  }

  /**
   * Sets the operation sequence number.
   *
   * @param sequence The operation sequence number.
   * @return The operation entry.
   */
  @SuppressWarnings("unchecked")
  public T setSequence(long sequence) {
    this.sequence = sequence;
    return (T) this;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(sequence);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    sequence = buffer.readLong();
  }

}
