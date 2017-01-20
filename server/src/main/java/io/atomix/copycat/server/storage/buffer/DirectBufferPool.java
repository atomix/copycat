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

import io.atomix.copycat.util.concurrent.ReferenceFactory;
import io.atomix.copycat.util.concurrent.ReferenceManager;

/**
 * Direct buffer pool.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class DirectBufferPool extends BufferPool {
  public DirectBufferPool() {
    super(new DirectBufferFactory());
  }

  @Override
  public void release(Buffer reference) {
    reference.rewind();
    super.release(reference);
  }

  /**
   * Direct buffer factory.
   */
  private static class DirectBufferFactory implements ReferenceFactory<Buffer> {
    @Override
    public Buffer createReference(ReferenceManager<Buffer> manager) {
      DirectBuffer buffer = new DirectBuffer(DirectBytes.allocate(1024), manager);
      buffer.reset(0, 1024, Long.MAX_VALUE);
      return buffer;
    }
  }

}
