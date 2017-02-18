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

import io.atomix.copycat.protocol.response.AbstractResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.server.protocol.response.AcceptResponse;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

public class AcceptResponseSerializer extends RaftProtocolResponseSerializer<AcceptResponse> {
  @Override
  public void writeObject(BufferOutput output, AcceptResponse response) {
    output.writeByte(response.status().id());
    if (response.status() == ProtocolResponse.Status.ERROR) {
      output.writeByte(response.error().type().id());
      output.writeString(response.error().message());
    }
  }

  @Override
  public AcceptResponse readObject(BufferInput input, Class<AcceptResponse> type) {
    final ProtocolResponse.Status status = ProtocolResponse.Status.forId(input.readByte());
    if (status == ProtocolResponse.Status.OK) {
      return new AcceptResponse(status, null);
    } else {
      ProtocolResponse.Error error = new AbstractResponse.Error(ProtocolResponse.Error.Type.forId(input.readByte()), input.readString());
      return new AcceptResponse(status, error);
    }
  }
}