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

import io.atomix.copycat.protocol.response.AbstractResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.response.QueryResponse;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

public class QueryResponseSerializer extends ProtocolResponseSerializer<QueryResponse> {
  @Override
  public void writeObject(BufferOutput output, QueryResponse response) {
    output.writeByte(response.status().id());
    if (response.status() == ProtocolResponse.Status.ERROR) {
      output.writeByte(response.error().type().id());
      output.writeString(response.error().message());
    }
    output.writeLong(response.index());
    output.writeLong(response.eventIndex());
    output.writeInt(response.result().length);
    output.write(response.result());
  }

  @Override
  public QueryResponse readObject(BufferInput input, Class<QueryResponse> type) {
    final ProtocolResponse.Status status = ProtocolResponse.Status.forId(input.readByte());
    ProtocolResponse.Error error = null;
    if (status == ProtocolResponse.Status.ERROR) {
      error = new AbstractResponse.Error(ProtocolResponse.Error.Type.forId(input.readByte()), input.readString());
    }
    return new QueryResponse(status, error, input.readLong(), input.readLong(), input.readBytes(input.readInt()));
  }
}