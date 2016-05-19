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

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.reference.ReferenceManager;
import io.atomix.copycat.Operation;
import io.atomix.copycat.Query;

/**
 * Represents a state machine {@link Query}.
 * <p>
 * The {@code QueryEntry} is a special entry that is typically not ever written to the Raft log.
 * Query entries are simply used to represent the context within which a query is applied to the
 * state machine. Query entry {@link #getSequence() sequence} numbers and {@link #getIndex() indexes}
 * are used to sequence queries as they're applied to the user state machine.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueryEntry extends OperationEntry<QueryEntry> {
  private Query query;

  public QueryEntry() {
  }

  public QueryEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  @Override
  public Operation getOperation() {
    return query;
  }

  /**
   * Returns the query.
   *
   * @return The query.
   */
  public Query getQuery() {
    return query;
  }

  /**
   * Sets the query.
   *
   * @param query The query.
   * @return The query entry.
   * @throws NullPointerException if {@code query} is null
   */
  public QueryEntry setQuery(Query query) {
    this.query = Assert.notNull(query, "query");
    return this;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    serializer.writeObject(query, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    query = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, session=%d, sequence=%d, timestamp=%d, query=%s]", getClass().getSimpleName(), getIndex(), getTerm(), getSession(), getSequence(), getTimestamp(), query);
  }

}
