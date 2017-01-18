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
package io.atomix.copycat.protocol.response;

import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.websocket.request.WebSocketQueryRequest;

/**
 * Client query response.
 * <p>
 * Query responses are sent by servers to clients upon the completion of a
 * {@link WebSocketQueryRequest}. Query responses are sent with the
 * {@link #index()} of the state machine at the point at which the query was evaluated.
 * This can be used by the client to ensure it sees state progress monotonically. Note, however, that
 * query responses may not be sent or received in sequential order. If a query response is proxied through
 * another server, responses may be received out of order. Clients should resequence concurrent responses
 * to ensure they're handled in FIFO order.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class QueryResponse extends OperationResponse {
  protected QueryResponse(Status status, CopycatError error, long index, long eventIndex, Object result) {
    super(status, error, index, eventIndex, result);
  }

  /**
   * Query response builder.
   */
  public static class Builder extends OperationResponse.Builder<QueryResponse.Builder, QueryResponse> {
    @Override
    public QueryResponse copy(QueryResponse response) {
      return new QueryResponse(response.status, response.error, response.index, response.eventIndex, response.result);
    }

    @Override
    public QueryResponse build() {
      return new QueryResponse(status, error, index, eventIndex, result);
    }
  }
}
