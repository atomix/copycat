/*
 * Copyright 2016 the original author or authors.
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
 * limitations under the License
 */
package io.atomix.copycat.server.storage;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.server.storage.entry.Entry;

/**
 * Indexed log entry.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Indexed<T extends Entry<T>> {
  private final long index;
  private final long term;
  private final T entry;
  private final int size;
  final EntryCleaner cleaner;

  public Indexed(long index, long term, T entry, int size) {
    this(index, term, entry, size, null);
  }

  public Indexed(long index, long term, T entry, int size, EntryCleaner cleaner) {
    this.index = index;
    this.term = term;
    this.entry = entry;
    this.size = size;
    this.cleaner = cleaner;
  }

  /**
   * Returns the entry index.
   *
   * @return The entry index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the entry term.
   *
   * @return The entry term.
   */
  public long term() {
    return term;
  }

  /**
   * Returns the entry type.
   *
   * @return The entry type.
   */
  public Entry.Type<T> type() {
    return entry.type();
  }

  /**
   * Returns the indexed entry.
   *
   * @return The indexed entry.
   */
  public T entry() {
    return entry;
  }

  /**
   * Returns the serialized entry size.
   *
   * @return The serialized entry size.
   */
  public int size() {
    return size;
  }

  /**
   * Returns the entry offset.
   *
   * @return The entry offset is {@code -1} if the offset is unknown.
   */
  public long offset() {
    return cleaner != null ? cleaner.offset : -1;
  }

  /**
   * Cleans the entry from the log.
   */
  public void clean() {
    if (cleaner == null) {
      throw new IllegalStateException("Cannot clean entry");
    } else {
      cleaner.clean();
    }
  }

  /**
   * Returns a boolean indicating whether the entry has been cleaned from the log.
   *
   * @return Indicates whether the entry has been cleaned from the log.
   */
  public boolean isClean() {
    return cleaner != null && cleaner.isClean();
  }

  /**
   * Returns a boolean indicating whether the entry has been committed to the log.
   *
   * @return Indicates whether the entry has been committed to the log.
   */
  public boolean isCommitted() {
    return entry == null || cleaner != null;
  }

  /**
   * Returns a boolean indicating whether the entry has been compacted from the log.
   *
   * @return Indicates whether the entry has been compacted from the log.
   */
  public boolean isCompacted() {
    return entry == null;
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, entry=%s]", getClass().getSimpleName(), index, term, entry);
  }

  /**
   * Indexed entry serializer.
   */
  public static class Serializer extends com.esotericsoftware.kryo.Serializer<Indexed<?>> {
    @Override
    public void write(Kryo kryo, Output output, Indexed<?> entry) {
      output.writeLong(entry.index);
      output.writeLong(entry.term);
      output.writeByte(entry.entry.type().id());
      kryo.writeObject(output, entry.entry, entry.entry.type().serializer());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Indexed<?> read(Kryo kryo, Input input, Class<Indexed<?>> type) {
      long index = input.readLong();
      long term = input.readLong();
      Entry.Type<?> entryType = Entry.Type.forId(input.readByte());
      return new Indexed(index, term, kryo.readObject(input, entryType.type(), entryType.serializer()), (int) input.total());
    }
  }
}
