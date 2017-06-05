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
package io.atomix.copycat.protocol;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Query;

import java.util.Arrays;
import java.util.Objects;

/**
 * Client query request.
 * <p>
 * Query requests are submitted by clients to the Copycat cluster to commit {@link Query}s to
 * the replicated state machine. Each query request must be associated with a registered
 * {@link #session()} and have a unique {@link #sequence()} number within that session. Queries will
 * be applied in the cluster in the order defined by the provided sequence number. Thus, sequence numbers
 * should never be skipped. In the event of a failure of a query request, the request should be resent
 * with the same sequence number. Queries are guaranteed to be applied in sequence order.
 * <p>
 * Query requests should always be submitted to the server to which the client is connected. The provided
 * query's {@link Query#consistency() consistency level} will be used to determine how the query should be
 * handled. If the query is received by a follower, it may be evaluated on that node if the consistency level
 * is {@link Query.ConsistencyLevel#SEQUENTIAL}, otherwise it will be forwarded to the cluster leader.
 * Queries are always guaranteed to see state progress monotonically within a single {@link #session()}
 * even when switching servers.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueryRequest extends OperationRequest {
  public static final String NAME = "query";

  /**
   * Returns a new query request builder.
   *
   * @return A new query request builder.
   */
  public static Builder builder() {
    return new Builder(new QueryRequest());
  }

  /**
   * Returns a query request builder for an existing request.
   *
   * @param request The request to build.
   * @return The query request builder.
   * @throws IllegalStateException if request is null
   */
  public static Builder builder(QueryRequest request) {
    return new Builder(request);
  }

  private long index;
  private Query.ConsistencyLevel consistency;

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
  public Query.ConsistencyLevel consistency() {
    return consistency;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    index = buffer.readLong();
    consistency = Query.ConsistencyLevel.values()[buffer.readByte()];
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(index);
    buffer.writeByte(consistency.ordinal());
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
        && request.consistency == consistency
        && Arrays.equals(request.bytes, bytes);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, sequence=%d, index=%d, consistency=%s, bytes=byte[%d]]", getClass().getSimpleName(), session, sequence, index, consistency, bytes.length);
  }

  /**
   * Query request builder.
   */
  public static class Builder extends OperationRequest.Builder<Builder, QueryRequest> {
    protected Builder(QueryRequest request) {
      super(request);
    }

    /**
     * Sets the request index.
     *
     * @param index The request index.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code index} is less than {@code 0}
     */
    public Builder withIndex(long index) {
      request.index = Assert.argNot(index, index < 0, "index cannot be less than 0");
      return this;
    }

    /**
     * Sets the query consistency level.
     *
     * @param consistency The query consistency level.
     * @return The request builder.
     */
    public Builder withConsistency(Query.ConsistencyLevel consistency) {
      request.consistency = Assert.notNull(consistency, "consistency");
      return this;
    }

    /**
     * @throws IllegalStateException if {@code query} is null
     */
    @Override
    public QueryRequest build() {
      super.build();
      Assert.stateNot(request.index < 0, "index cannot be less than 0");
      Assert.notNull(request.consistency, "consistency");
      return request;
    }
  }

}
