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

import io.atomix.copycat.protocol.request.PublishRequest;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

import java.util.ArrayList;
import java.util.List;

public class PublishRequestSerializer extends ProtocolRequestSerializer<PublishRequest> {
  @Override
  public void writeObject(BufferOutput output, PublishRequest request) {
    output.writeLong(request.session());
    output.writeLong(request.eventIndex());
    output.writeLong(request.previousIndex());
    output.writeInt(request.events().size());
    for (byte[] event : request.events()) {
      output.writeInt(event.length).write(event);
    }
  }

  @Override
  @SuppressWarnings("unchecked")
  public PublishRequest readObject(BufferInput input, Class<PublishRequest> type) {
    final long session = input.readLong();
    final long eventIndex = input.readLong();
    final long previousIndex = input.readLong();
    final int size = input.readInt();
    final List<byte[]> events = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      events.add(input.readBytes(input.readInt()));
    }
    return new PublishRequest(session, eventIndex, previousIndex, events);
  }
}