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
 * limitations under the License.
 */
package io.atomix.copycat.server.storage.entry;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Represents a state machine query.
 * <p>
 * The {@code QueryEntry} is a special entry that is typically not ever written to the Raft log.
 * Query entries are simply used to represent the context within which a query is applied to the
 * state machine. Query entry {@link #sequence() sequence} numbers and indexes
 * are used to sequence queries as they're applied to the user state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueryEntry extends OperationEntry<QueryEntry> {

  public QueryEntry(long timestamp, long session, long sequence, byte[] bytes) {
    super(timestamp, session, sequence, bytes);
  }

  @Override
  public Type<QueryEntry> type() {
    return Type.QUERY;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, sequence=%d, timestamp=%d, query=byte[%d]]", getClass().getSimpleName(), session(), sequence(), timestamp(), bytes.length);
  }

  /**
   * Query entry serializer.
   */
  public static class Serializer extends OperationEntry.Serializer<QueryEntry> {
    @Override
    public void write(Kryo kryo, Output output, QueryEntry entry) {
      output.writeLong(entry.timestamp);
      output.writeLong(entry.session);
      output.writeLong(entry.sequence);
      output.writeInt(entry.bytes.length);
      output.write(entry.bytes);
    }

    @Override
    public QueryEntry read(Kryo kryo, Input input, Class<QueryEntry> type) {
      return new QueryEntry(input.readLong(), input.readLong(), input.readLong(), input.readBytes(input.readInt()));
    }
  }
}
