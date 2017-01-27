/*
 * Copyright 2016 the original author or authors.
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
package io.atomix.copycat.protocol.request;

import io.atomix.copycat.ConsistencyLevel;
import io.atomix.copycat.util.Assert;

import java.util.Arrays;
import java.util.Objects;

/**
 * Client query request.
 * <p>
 * Query requests are submitted by clients to the Copycat cluster to commit queries to
 * the replicated state machine. Each query request must be associated with a registered
 * {@link #session()} and have a unique {@link #sequence()} number within that session. Queries will
 * be applied in the cluster in the order defined by the provided sequence number. Thus, sequence numbers
 * should never be skipped. In the event of a failure of a query request, the request should be resent
 * with the same sequence number. Queries are guaranteed to be applied in sequence order.
 * <p>
 * Query requests should always be submitted to the server to which the client is connected. The provided
 * query's {@link ConsistencyLevel consistency level} will be used to determine how the query should be
 * handled. If the query is received by a follower, it may be evaluated on that node if the consistency level
 * is {@link ConsistencyLevel#SEQUENTIAL},
 * otherwise it will be forwarded to the cluster leader. Queries are always guaranteed to see state progress
 * monotonically within a single {@link #session()} even when switching servers.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueryRequest extends OperationRequest {
  protected final long index;
  protected final ConsistencyLevel consistency;

  protected QueryRequest(long session, long sequence, long index, byte[] bytes, ConsistencyLevel consistency) {
    super(session, sequence, bytes);
    this.index = Assert.argNot(index, index < 0, "index cannot be less than 0");
    this.consistency = Assert.notNull(consistency, "consistency");
  }

  /**
   * Returns the query index.
   *
   * @return The query index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the query consistency level.
   *
   * @return The query consistency level.
   */
  public ConsistencyLevel consistency() {
    return consistency;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, sequence, index, bytes);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof QueryRequest) {
      QueryRequest request = (QueryRequest) object;
      return request.session == session
        && request.sequence == sequence
        && Arrays.equals(request.bytes, bytes);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, sequence=%d, index=%d, query=byte[%d]]", getClass().getSimpleName(), session, sequence, index, bytes.length);
  }

  /**
   * Query request builder.
   */
  public static class Builder extends OperationRequest.Builder<QueryRequest.Builder, QueryRequest> {
    protected long index;
    protected byte[] bytes;
    protected ConsistencyLevel consistency = ConsistencyLevel.LINEARIZABLE;

    /**
     * Sets the request index.
     *
     * @param index The request index.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code index} is less than {@code 0}
     */
    public Builder withIndex(long index) {
      this.index = Assert.argNot(index, index < 0, "index cannot be less than 0");
      return this;
    }

    /**
     * Sets the request query.
     *
     * @param bytes The request query bytes.
     * @return The request builder.
     * @throws NullPointerException if {@code bytes} is null
     */
    public Builder withQuery(byte[] bytes) {
      this.bytes = Assert.notNull(bytes, "bytes");
      return this;
    }

    /**
     * Sets the query consistency level.
     *
     * @param consistency The query consistency level.
     * @return The request builder.
     * @throws NullPointerException if {@code consistency} is null
     */
    public Builder withConsistency(ConsistencyLevel consistency) {
      this.consistency = Assert.notNull(consistency, "consistency");
      return this;
    }

    @Override
    public QueryRequest copy(QueryRequest request) {
      return new QueryRequest(request.session, request.sequence, request.index, request.bytes, request.consistency);
    }

    @Override
    public QueryRequest build() {
      return new QueryRequest(session, sequence, index, bytes, consistency);
    }
  }
}
