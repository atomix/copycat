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
 * limitations under the License
 */
package io.atomix.copycat.server.storage.snapshot;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import io.atomix.copycat.server.storage.buffer.Buffer;
import io.atomix.copycat.server.storage.buffer.BufferInput;
import io.atomix.copycat.server.storage.buffer.Bytes;
import io.atomix.copycat.util.Assert;

/**
 * Reads bytes from a state machine {@link Snapshot}.
 * <p>
 * This class provides the primary interface for reading snapshot buffers from disk or memory.
 * Snapshot bytes are read from an underlying {@link Buffer} which is backed by either memory
 * or disk based on the configured {@link io.atomix.copycat.server.storage.StorageLevel}.
 * <p>
 * In addition to standard {@link BufferInput} methods, snapshot readers support reading serializable objects
 * from the snapshot via the {@link #readObject()} method. Serializable types must be registered on the
 * {@link io.atomix.copycat.server.CopycatServer} serializer to be supported in snapshots.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class SnapshotReader implements BufferInput<SnapshotReader> {
  private final Buffer buffer;
  private final Snapshot snapshot;
  private final Kryo serializer;

  SnapshotReader(Buffer buffer, Snapshot snapshot, Kryo serializer) {
    this.buffer = Assert.notNull(buffer, "buffer");
    this.snapshot = Assert.notNull(snapshot, "snapshot");
    this.serializer = Assert.notNull(serializer, "serializer");
  }

  @Override
  public long remaining() {
    return buffer.remaining();
  }

  @Override
  public boolean hasRemaining() {
    return buffer.hasRemaining();
  }

  @Override
  public SnapshotReader skip(long bytes) {
    buffer.skip(bytes);
    return this;
  }

  /**
   * Reads an object from the buffer.
   *
   * @param <T> The type of the object to read.
   * @return The read object.
   */
  @SuppressWarnings("unchecked")
  public <T> T readObject() {
    byte[] bytes = new byte[(int) buffer.remaining()];
    buffer.read(bytes);
    return (T) serializer.readClassAndObject(new Input(bytes));
  }

  @Override
  public SnapshotReader read(Bytes bytes) {
    buffer.read(bytes);
    return this;
  }

  @Override
  public SnapshotReader read(byte[] bytes) {
    buffer.read(bytes);
    return this;
  }

  @Override
  public SnapshotReader read(Bytes bytes, long offset, long length) {
    buffer.read(bytes, offset, length);
    return this;
  }

  @Override
  public SnapshotReader read(byte[] bytes, long offset, long length) {
    buffer.read(bytes, offset, length);
    return this;
  }

  @Override
  public SnapshotReader read(Buffer buffer) {
    this.buffer.read(buffer);
    return this;
  }

  @Override
  public int readByte() {
    return buffer.readByte();
  }

  @Override
  public int readUnsignedByte() {
    return buffer.readUnsignedByte();
  }

  @Override
  public char readChar() {
    return buffer.readChar();
  }

  @Override
  public short readShort() {
    return buffer.readShort();
  }

  @Override
  public int readUnsignedShort() {
    return buffer.readUnsignedShort();
  }

  @Override
  public int readMedium() {
    return buffer.readMedium();
  }

  @Override
  public int readUnsignedMedium() {
    return buffer.readUnsignedMedium();
  }

  @Override
  public int readInt() {
    return buffer.readInt();
  }

  @Override
  public long readUnsignedInt() {
    return buffer.readUnsignedInt();
  }

  @Override
  public long readLong() {
    return buffer.readLong();
  }

  @Override
  public float readFloat() {
    return buffer.readFloat();
  }

  @Override
  public double readDouble() {
    return buffer.readDouble();
  }

  @Override
  public boolean readBoolean() {
    return buffer.readBoolean();
  }

  @Override
  public String readString() {
    return buffer.readString();
  }

  @Override
  public String readUTF8() {
    return buffer.readUTF8();
  }

  @Override
  public void close() {
    buffer.close();
    snapshot.closeReader(this);
  }

}
