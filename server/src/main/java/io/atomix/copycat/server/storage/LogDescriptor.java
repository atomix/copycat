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
package io.atomix.copycat.server.storage;

import io.atomix.catalyst.buffer.Buffer;
import io.atomix.catalyst.util.Assert;

/**
 * Log descriptor.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class LogDescriptor implements AutoCloseable {
  private final String name;
  private final Buffer buffer;

  LogDescriptor(String name, Buffer buffer) {
    this.name = Assert.notNull(name, "name");
    this.buffer = Assert.notNull(buffer, "buffer");
  }

  /**
   * Returns the log name.
   *
   * @return The log name.
   */
  public String name() {
    return name;
  }

  /**
   * Returns the log term.
   *
   * @return The log term.
   */
  public long term() {
    return buffer.readLong(0);
  }

  /**
   * Writes the log term.
   *
   * @param term The log term.
   */
  void term(long term) {
    buffer.writeLong(0, term);
  }

  /**
   * Returns the last vote cast by the log.
   *
   * @return The last vote cast.
   */
  public int vote() {
    return buffer.readInt(8);
  }

  /**
   * Writes the candidate voted for.
   *
   * @param candidate The candidate voted for.
   */
  void vote(int candidate) {
    buffer.writeInt(8, candidate);
  }

  /**
   * Flushes the descriptor buffer.
   */
  void flush() {
    buffer.flush();
  }

  @Override
  public void close() {
    buffer.close();
  }

}
