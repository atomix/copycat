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

import io.atomix.copycat.protocol.http.HttpServerConnection;
import io.atomix.copycat.protocol.request.UnregisterRequest;
import io.atomix.copycat.protocol.response.UnregisterResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

/**
 * Close session handler.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class CloseSessionHandler extends RequestHandler {

  /**
   * Close session request handler factory.
   */
  public static class Factory implements RequestHandler.Factory {
    @Override
    public RequestHandler create(HttpServerConnection connection) {
      return new CloseSessionHandler(connection);
    }
  }

  public CloseSessionHandler(HttpServerConnection connection) {
    super(connection);
  }

  @Override
  public void register(Router router) {
    router.route()
      .path("/sessions/:sessionId")
      .method(HttpMethod.DELETE)
      .handler(this);
  }

  @Override
  public void handle(RoutingContext context) {
    final long session = Long.valueOf(context.pathParam("sessionId"));
    connection.onUnregister(new UnregisterRequest.Builder()
      .withSession(session)
      .build(), new UnregisterResponse.Builder()).whenComplete((response, error) -> {
      if (error == null) {
        succeed(context, 202);
      } else {
        fail(context, error, 500);
      }
    });
  }
}
