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
package io.atomix.copycat.server.storage;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.copycat.server.storage.entry.Entry;

/**
 * Test entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TestEntry extends Entry<TestEntry> {
  private final byte[] bytes;

  public TestEntry() {
    this(new byte[0]);
  }

  public TestEntry(byte[] bytes) {
    this.bytes = bytes;
  }

  @Override
  public Type<TestEntry> type() {
    return new Type<>(10, TestEntry.class, new Serializer<TestEntry>() {
      @Override
      public void writeObject(BufferOutput output, TestEntry object) {
        output.writeInt(bytes.length);
      }

      @Override
      public TestEntry readObject(BufferInput input, Class<TestEntry> type) {
        return new TestEntry(new byte[input.readInt()]);
      }
    });
  }

  @Override
  public String toString() {
    return String.format("%s[bytes=byte[%d]]", getClass().getSimpleName(), bytes.length);
  }

}
