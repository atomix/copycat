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
import io.atomix.copycat.protocol.http.response.HttpEventResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

/**
 * Event request handler.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class EventHandler extends RequestHandler {

  /**
   * Event request handler factory.
   */
  public static class Factory implements RequestHandler.Factory {
    @Override
    public RequestHandler create(HttpServerConnection connection) {
      return new EventHandler(connection);
    }
  }

  public EventHandler(HttpServerConnection connection) {
    super(connection);
  }

  @Override
  public void register(Router router) {
    router.route()
      .path("/session/:sessionId/events")
      .method(HttpMethod.GET)
      .produces("application/json");
  }

  @Override
  public void handle(RoutingContext context) {
    long sessionId = Long.valueOf(context.pathParam("sessionId"));
    connection.onEvent(sessionId, request -> {
      HttpEventResponse response = new HttpEventResponse()
        .setSession(request.session())
        .setEvents(request.events());
      succeed(context, response, 200);
    });
  }
}
