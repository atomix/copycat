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

import io.atomix.copycat.ConsistencyLevel;
import io.atomix.copycat.protocol.request.QueryRequest;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.BufferOutput;

public class QueryRequestSerializer extends ProtocolRequestSerializer<QueryRequest> {
  @Override
  public void writeObject(BufferOutput output, QueryRequest request) {
    output.writeLong(request.session());
    output.writeLong(request.sequence());
    output.writeLong(request.index());
    output.writeInt(request.bytes().length);
    output.write(request.bytes());
    output.writeByte(request.consistency().id);
  }

  @Override
  public QueryRequest readObject(BufferInput input, Class<QueryRequest> type) {
    return new QueryRequest(input.readLong(), input.readLong(), input.readLong(), input.readBytes(input.readInt()), ConsistencyLevel.forId(input.readByte()));
  }
}