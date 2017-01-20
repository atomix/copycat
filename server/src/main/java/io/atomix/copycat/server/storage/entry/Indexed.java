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
package io.atomix.copycat.server.storage.entry;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Indexed log entry.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Indexed<T extends Entry> {
  private final long index;
  private final long term;
  private final T entry;
  private final int size;

  public Indexed(long index, long term, T entry, int size) {
    this.index = index;
    this.term = term;
    this.entry = entry;
    this.size = size;
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
    public Indexed<?> read(Kryo kryo, Input input, Class<Indexed<?>> type) {
      long index = input.readLong();
      long term = input.readLong();
      Entry.Type<?> entryType = Entry.Type.forId(input.readByte());
      return new Indexed<>(index, term, kryo.readObject(input, entryType.type(), entryType.serializer()), (int) input.total());
    }
  }
}
