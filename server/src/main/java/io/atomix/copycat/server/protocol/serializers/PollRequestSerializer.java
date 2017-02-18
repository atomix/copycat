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
package io.atomix.copycat.server.protocol.serializers;

import io.atomix.copycat.server.protocol.request.PollRequest;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

public class PollRequestSerializer extends RaftProtocolRequestSerializer<PollRequest> {
  @Override
  public void writeObject(BufferOutput output, PollRequest request) {
    output.writeLong(request.term());
    output.writeInt(request.candidate());
    output.writeLong(request.logIndex());
    output.writeLong(request.logTerm());
  }

  @Override
  public PollRequest readObject(BufferInput input, Class<PollRequest> type) {
    return new PollRequest(input.readLong(), input.readInt(), input.readLong(), input.readLong());
  }
}