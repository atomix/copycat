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
package io.atomix.copycat.protocol.tcp;

import io.atomix.copycat.util.buffer.Buffer;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.Bytes;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;

/**
 * Byte buffer input.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ByteBufInput implements BufferInput<ByteBufInput> {
  ByteBuf buffer;

  /**
   * Sets the underlying byte buffer.
   */
  public ByteBufInput setByteBuf(ByteBuf buffer) {
    this.buffer = buffer;
    return this;
  }

  @Override
  public long position() {
    return buffer.readerIndex();
  }

  @Override
  public long remaining() {
    return buffer.readableBytes();
  }

  @Override
  public boolean hasRemaining() {
    return remaining() > 0;
  }

  @Override
  public ByteBufInput skip(long bytes) {
    buffer.readerIndex(buffer.readerIndex() + (int) bytes);
    return this;
  }

  @Override
  public ByteBufInput read(Buffer buffer) {
    byte[] bytes = new byte[this.buffer.readableBytes()];
    this.buffer.readBytes(bytes);
    buffer.write(bytes);
    return this;
  }

  @Override
  public ByteBufInput read(Bytes bytes) {
    byte[] b = new byte[Math.min((int) bytes.size(), buffer.readableBytes())];
    buffer.readBytes(b);
    bytes.write(0, b, 0, b.length);
    return this;
  }

  @Override
  public ByteBufInput read(byte[] bytes) {
    buffer.readBytes(bytes);
    return this;
  }

  @Override
  public ByteBufInput read(Bytes bytes, long dstOffset, long length) {
    byte[] b = new byte[Math.min((int) length, buffer.readableBytes())];
    buffer.readBytes(b);
    bytes.write(dstOffset, b, 0, b.length);
    return this;
  }

  @Override
  public ByteBufInput read(byte[] bytes, long offset, long length) {
    buffer.readBytes(bytes, (int) offset, (int) length);
    return this;
  }

  @Override
  public byte[] readBytes() {
    byte[] bytes = new byte[(int) remaining()];
    read(bytes);
    return bytes;
  }

  @Override
  public byte[] readBytes(int length) {
    byte[] bytes = new byte[length];
    read(bytes);
    return bytes;
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
  public int readInt() {
    return buffer.readInt();
  }

  @Override
  public long readUnsignedInt() {
    return buffer.readUnsignedInt();
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
    return readUTF8();
  }

  @Override
  public String readUTF8() {
    int nullByte = buffer.readByte();
    if (nullByte == 0) {
      return null;
    } else {
      byte[] bytes = new byte[buffer.readUnsignedShort()];
      buffer.readBytes(bytes);
      return new String(bytes, StandardCharsets.UTF_8);
    }
  }

  @Override
  public void close() {

  }

}
