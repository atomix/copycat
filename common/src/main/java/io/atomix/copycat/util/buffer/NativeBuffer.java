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
package io.atomix.copycat.util.buffer;

import io.atomix.copycat.util.buffer.util.NativeMemory;
import io.atomix.copycat.util.concurrent.ReferenceManager;

/**
 * Native byte buffer implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class NativeBuffer extends AbstractBuffer {

  protected NativeBuffer(NativeBytes bytes, ReferenceManager<Buffer> referenceManager) {
    super(bytes, referenceManager);
  }

  protected NativeBuffer(NativeBytes bytes, long offset, long initialCapacity, long maxCapacity) {
    super(bytes, offset, initialCapacity, maxCapacity, null);
  }

  @Override
  protected void compact(long from, long to, long length) {
    NativeMemory memory = ((NativeBytes) bytes).memory;
    memory.unsafe().copyMemory(memory.address(from), memory.address(to), length);
    memory.unsafe().setMemory(memory.address(from), length, (byte) 0);
  }

}
