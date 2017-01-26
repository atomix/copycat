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
import io.atomix.copycat.Operation;
import io.atomix.copycat.Query;
import io.atomix.copycat.util.Assert;

/**
 * Represents a state machine {@link Query}.
 * <p>
 * The {@code QueryEntry} is a special entry that is typically not ever written to the Raft log.
 * Query entries are simply used to represent the context within which a query is applied to the
 * state machine. Query entry {@link #sequence() sequence} numbers and indexes
 * are used to sequence queries as they're applied to the user state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueryEntry extends OperationEntry<QueryEntry> {
  private final Query query;

  public QueryEntry(long timestamp, long session, long sequence, Query query) {
    super(timestamp, session, sequence);
    this.query = Assert.notNull(query, "query");
  }

  @Override
  public Type<QueryEntry> type() {
    return Type.QUERY;
  }

  @Override
  public Operation operation() {
    return query;
  }

  /**
   * Returns the query.
   *
   * @return The query.
   */
  public Query query() {
    return query;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, sequence=%d, timestamp=%d, query=%s]", getClass().getSimpleName(), session(), sequence(), timestamp(), query);
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
      kryo.writeClassAndObject(output, entry.query);
    }

    @Override
    public QueryEntry read(Kryo kryo, Input input, Class<QueryEntry> type) {
      return new QueryEntry(input.readLong(), input.readLong(), input.readLong(), (Query) kryo.readClassAndObject(input));
    }
  }
}
