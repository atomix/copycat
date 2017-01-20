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
package io.atomix.copycat.server.storage.buffer;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class BufferOutputStream extends OutputStream {
  private final BufferOutput<?> buffer;

  public BufferOutputStream(BufferOutput<?> buffer) {
    this.buffer = buffer;
  }

  @Override
  public void write(int b) throws IOException {
    buffer.writeByte(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    buffer.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    buffer.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    buffer.flush();
  }

  @Override
  public void close() throws IOException {
    buffer.close();
  }

}
