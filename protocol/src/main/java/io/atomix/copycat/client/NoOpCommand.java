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
package io.atomix.copycat.client;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;

/**
 * The no-op command is a special command implementation that represents a gap in the sequence of commands.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@SerializeWith(id=189)
public class NoOpCommand<T> implements Command<T>, CatalystSerializable {
  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof NoOpCommand;
  }

  @Override
  public int hashCode() {
    return 37 * getClass().getName().hashCode() + 17;
  }

  @Override
  public String toString() {
    return "NoOpCommand";
  }

}
