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
package io.atomix.copycat.protocol.http.handlers;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.undercouch.bson4jackson.BsonFactory;
import io.atomix.copycat.ConsistencyLevel;
import io.atomix.copycat.protocol.http.HttpQuery;
import io.atomix.copycat.protocol.http.HttpServerConnection;
import io.atomix.copycat.protocol.request.QueryRequest;
import io.atomix.copycat.protocol.response.QueryResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Query handler.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class QueryHandler extends RequestHandler {

  /**
   * Query request handler factory.
   */
  public static class Factory implements RequestHandler.Factory {
    @Override
    public RequestHandler create(HttpServerConnection connection) {
      return new QueryHandler(connection);
    }
  }

  private final ObjectMapper mapper = new ObjectMapper(new BsonFactory());

  public QueryHandler(HttpServerConnection connection) {
    super(connection);
  }

  @Override
  public void register(Router router) {
    router.route()
      .pathRegex("/sessions/:sessionId/*")
      .method(HttpMethod.GET)
      .produces("application/json")
      .handler(this);
  }

  @Override
  public void handle(RoutingContext context) {
    final long sessionId = Long.valueOf(context.pathParam("sessionId"));

    final String path = context.pathParam("path");
    final String consistency = context.pathParam("consistency");
    final Map<String, String> headers = new HashMap<>();
    for (Map.Entry<String, String> entry : context.request().headers().entries()) {
      headers.put(entry.getKey(), entry.getValue());
    }

    final HttpQuery query = new HttpQuery(path, headers, context.getBodyAsString());
    final byte[] bytes;
    try {
      bytes = mapper.writeValueAsBytes(query);
    } catch (IOException e) {
      fail(context, e, 400);
      return;
    }

    connection.onQuery(new QueryRequest.Builder()
      .withSession(sessionId)
      .withQuery(bytes)
      .withConsistency(consistency != null ? ConsistencyLevel.valueOf(consistency.toUpperCase()) : ConsistencyLevel.LINEARIZABLE_LEASE)
      .build(), new QueryResponse.Builder())
      .whenComplete((response, error) -> {
        if (error == null) {
          succeed(context, Json.encode(response.result()), 200);
        } else {
          fail(context, error, 500);
        }
      });
  }
}
