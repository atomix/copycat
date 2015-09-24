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
package io.atomix.catalog.client.request;

import io.atomix.catalog.client.Operation;
import io.atomix.catalog.client.Query;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.BuilderPool;
import io.atomix.catalyst.util.ReferenceManager;

import java.util.Objects;

/**
 * Protocol query request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=258)
public class QueryRequest extends OperationRequest<QueryRequest> {

  private static final BuilderPool<Builder, QueryRequest> POOL = new BuilderPool<>(Builder::new);

  /**
   * Returns a new query request builder.
   *
   * @return A new query request builder.
   */
  public static Builder builder() {
    return POOL.acquire();
  }

  /**
   * Returns a query request builder for an existing request.
   *
   * @param request The request to build.
   * @return The query request builder.
   * @throws IllegalStateException if request is null
   */
  public static Builder builder(QueryRequest request) {
    return POOL.acquire(Assert.notNull(request, "request"));
  }

  private long version;
  private Query query;

  /**
   * @throws NullPointerException if {@code referenceManager} is null
   */
  public QueryRequest(ReferenceManager<QueryRequest> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the query version.
   *
   * @return The query version.
   */
  public long version() {
    return version;
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
  public Operation operation() {
    return query;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    version = buffer.readLong();
    query = serializer.readObject(buffer);
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(version);
    serializer.writeObject(query, buffer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, sequence, version, query);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof QueryRequest) {
      QueryRequest request = (QueryRequest) object;
      return request.session == session
        && request.sequence == sequence
        && request.query.equals(query);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, sequence=%d, version=%d, query=%s]", getClass().getSimpleName(), session, sequence, version, query);
  }

  /**
   * Query request builder.
   */
  public static class Builder extends OperationRequest.Builder<Builder, QueryRequest> {

    /**
     * @throws NullPointerException if {@code pool} is null
     */
    protected Builder(BuilderPool<Builder, QueryRequest> pool) {
      super(pool, QueryRequest::new);
    }

    @Override
    protected void reset() {
      super.reset();
      request.version = 0;
      request.query = null;
    }

    /**
     * Sets the request version.
     *
     * @param version The request version.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code version} is less than {@code 0}
     */
    public Builder withVersion(long version) {
      request.version = Assert.argNot(version, version < 0, "version cannot be less than 0");
      return this;
    }

    /**
     * Sets the request query.
     *
     * @param query The request query.
     * @return The request builder.
     * @throws NullPointerException if {@code query} is null
     */
    public Builder withQuery(Query query) {
      request.query = Assert.notNull(query, "query");
      return this;
    }

    /**
     * @throws IllegalStateException if {@code query} is null
     */
    @Override
    public QueryRequest build() {
      super.build();
      Assert.stateNot(request.version < 0, "version cannot be less than 0");
      Assert.stateNot(request.query == null, "query cannot be null");
      return request;
    }
  }

}
