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
package io.atomix.copycat.protocol.websocket.request;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.Operation;
import io.atomix.copycat.Query;
import io.atomix.copycat.protocol.request.QueryRequest;

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
 * is {@link Query.ConsistencyLevel#SEQUENTIAL},
 * otherwise it will be forwarded to the cluster leader. Queries are always guaranteed to see state progress
 * monotonically within a single {@link #session()} even when switching servers.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class WebSocketQueryRequest extends WebSocketOperationRequest implements QueryRequest {
  @JsonProperty("index")
  private final long index;
  @JsonProperty("query")
  private final Query query;

  @JsonCreator
  protected WebSocketQueryRequest(@JsonProperty("id") long id, @JsonProperty("session") long session, @JsonProperty("sequence") long sequence, @JsonProperty("index") long index, @JsonProperty("query") Query query) {
    super(id, session, sequence);
    this.index = index;
    this.query = query;
  }

  @Override
  @JsonGetter("type")
  public Type type() {
    return Type.QUERY_REQUEST;
  }

  @Override
  @JsonGetter("index")
  public long index() {
    return index;
  }

  @Override
  @JsonGetter("query")
  public Query query() {
    return query;
  }

  @Override
  public Operation operation() {
    return query;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, sequence, index, query);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof WebSocketQueryRequest) {
      WebSocketQueryRequest request = (WebSocketQueryRequest) object;
      return request.session == session
        && request.sequence == sequence
        && request.query.equals(query);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, sequence=%d, index=%d, query=%s]", getClass().getSimpleName(), session, sequence, index, query);
  }

  /**
   * Query request builder.
   */
  public static class Builder extends WebSocketOperationRequest.Builder<QueryRequest.Builder, QueryRequest> implements QueryRequest.Builder {
    private long index;
    private Query query;

    public Builder(long id) {
      super(id);
    }

    @Override
    public Builder withIndex(long index) {
      this.index = Assert.argNot(index, index < 0, "index cannot be less than 0");
      return this;
    }

    @Override
    public Builder withQuery(Query query) {
      this.query = Assert.notNull(query, "query");
      return this;
    }

    @Override
    public QueryRequest build() {
      return new WebSocketQueryRequest(id, session, sequence, index, query);
    }
  }
}
