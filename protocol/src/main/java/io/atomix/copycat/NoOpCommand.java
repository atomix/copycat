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
package io.atomix.copycat;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;

/**
 * Special placeholder command representing a client operation that has no effect on the state machine.
 * <p>
 * No-op commands are submitted by clients to the cluster to complete missing command sequence numbers.
 * Copycat clusters require that clients submit commands in sequential order and use a client-provided
 * sequence number to ensure FIFO ordering of operations submitted to the cluster. In the event that a
 * command fails to be committed to the cluster, a client can resubmit a no-op command to ensure command
 * sequence numbers continue to progress.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NoOpCommand implements Command<Void>, CatalystSerializable {

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
  }

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
