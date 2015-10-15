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

import org.testng.annotations.Factory;
import org.testng.annotations.Test;

/**
 * In-memory log test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class MemoryLogTest extends LogTest {
  @Factory
  public Object[] createTests() throws Throwable {
    return testsFor(MemoryLogTest.class);
  }

  @Override
  protected StorageLevel storageLevel() {
    return StorageLevel.MEMORY;
  }

}
