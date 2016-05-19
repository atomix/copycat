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
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.reference.ReferenceCounted;
import io.atomix.catalyst.util.reference.ReferenceManager;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.compaction.Compaction;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Stores a state change in a {@link Log}.
 * <p>
 * The {@code Entry} represents a single record in a Copycat {@link Log}. Each entry is stored at
 * a unique {@link #getIndex() index} in the log. Indexes are applied to entries once written to a log.
 * <p>
 * Custom entry implementations should implement serialization and deserialization logic via
 * {@link io.atomix.catalyst.serializer.CatalystSerializable#writeObject(io.atomix.catalyst.buffer.BufferOutput, io.atomix.catalyst.serializer.Serializer)}
 * and {@link io.atomix.catalyst.serializer.CatalystSerializable#readObject(io.atomix.catalyst.buffer.BufferInput, io.atomix.catalyst.serializer.Serializer)}.
 * respectively.
 * <p>
 * Because log entries may remain in memory for an arbitrary amount of time, {@code Entry} objects are
 * recycled by the log using the {@link TypedEntryPool}. The lifecycle of an entry is managed via the
 * {@link ReferenceCounted} interface. Once all references to an entry have been {@link #release() released}
 * the entry will be placed back in the pool.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class Entry<T extends Entry<T>> implements ReferenceCounted<Entry>, CatalystSerializable {
  private final ReferenceManager<Entry<?>> referenceManager;
  private final AtomicInteger references = new AtomicInteger();
  private long index;
  private long term;
  private int size = -1;

  protected Entry() {
    referenceManager = null;
  }

  protected Entry(ReferenceManager<Entry<?>> referenceManager) {
    this.referenceManager = referenceManager;
  }

  /**
   * Resets the entry state.
   */
  @SuppressWarnings("unchecked")
  protected T reset() {
    this.index = 0;
    this.size = -1;
    return (T) this;
  }

  /**
   * Returns the entry index.
   *
   * @return The entry index.
   */
  public long getIndex() {
    return index;
  }

  /**
   * Sets the entry index.
   *
   * @param index The entry index.
   * @return The entry.
   */
  @SuppressWarnings("unchecked")
  public T setIndex(long index) {
    this.index = index;
    return (T) this;
  }

  /**
   * Returns the entry compaction mode.
   *
   * @return The entry compaction mode.
   */
  public Compaction.Mode getCompactionMode() {
    return Compaction.Mode.QUORUM;
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
  public T setTerm(long term) {
    this.term = term;
    return (T) this;
  }

  /**
   * Returns the entry size.
   *
   * @return The entry size.
   * @throws IllegalStateException If the entry has not yet been persisted
   */
  public int size() {
    Assert.stateNot(size == -1, "cannot determine size for non-persisted entry");
    return size;
  }

  /**
   * Sets the entry size.
   *
   * @param size The entry size.
   * @return The entry.
   */
  @SuppressWarnings("unchecked")
  public T setSize(int size) {
    this.size = size;
    return (T) this;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
  }

  @Override
  public Entry acquire() {
    references.incrementAndGet();
    return this;
  }

  @Override
  public boolean release() {
    int refs = references.decrementAndGet();
    if (refs == 0) {
      if (referenceManager != null)
        referenceManager.release(this);
      return true;
    } else if (refs < 0) {
      references.set(0);
      throw new IllegalStateException("cannot dereference non-referenced object");
    }
    return false;
  }

  @Override
  public int references() {
    return references.get();
  }

  @Override
  public void close() {
    release();
  }

}
