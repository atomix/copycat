/*
 * Copyright 2017 the original author or authors.
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
package io.atomix.copycat.protocol.serializers;

import io.atomix.copycat.protocol.request.CommandRequest;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

public class CommandRequestSerializer extends ProtocolRequestSerializer<CommandRequest> {
  @Override
  public void writeObject(BufferOutput output, CommandRequest request) {
    output.writeLong(request.session());
    output.writeLong(request.sequence());
    output.writeInt(request.bytes().length);
    output.write(request.bytes());
  }

  @Override
  public CommandRequest readObject(BufferInput input, Class<CommandRequest> type) {
    return new CommandRequest(input.readLong(), input.readLong(), input.readBytes(input.readInt()));
  }
}