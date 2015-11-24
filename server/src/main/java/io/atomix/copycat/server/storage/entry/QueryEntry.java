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
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ReferenceManager;
import io.atomix.copycat.client.Operation;
import io.atomix.copycat.client.Query;

/**
 * Query entry.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=225)
public class QueryEntry extends OperationEntry<QueryEntry> {
  private long version;
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
   * Returns the query version.
   *
   * @return The query version.
   */
  public long getVersion() {
    return version;
  }

  /**
   * Sets the query version.
   *
   * @param version The query version.
   * @return The query entry.
   */
  public QueryEntry setVersion(long version) {
    this.version = version;
    return this;
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
    buffer.writeLong(version);
    serializer.writeObject(query, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    version = buffer.readLong();
    query = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, session=%d, version=%d, timestamp=%d, query=%s]", getClass().getSimpleName(), getIndex(), getTerm(), getSession(), getSequence(), getTimestamp(), query);
  }

}
