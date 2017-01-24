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

import io.atomix.copycat.protocol.http.HttpCommand;
import io.atomix.copycat.protocol.http.HttpServerConnection;
import io.atomix.copycat.protocol.request.CommandRequest;
import io.atomix.copycat.protocol.response.CommandResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Command handler.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class CommandHandler extends RequestHandler {

  /**
   * Command request handler factory.
   */
  public static class Factory implements RequestHandler.Factory {
    @Override
    public RequestHandler create(HttpServerConnection connection) {
      return new CommandHandler(connection);
    }
  }

  public CommandHandler(HttpServerConnection connection) {
    super(connection);
  }

  @Override
  public void register(Router router) {
    router.route()
      .path("/sessions/:sessionId/:path")
      .method(HttpMethod.POST)
      .consumes("application/json")
      .produces("application/json")
      .handler(this);

    router.route()
      .path("/sessions/:sessionId/:path")
      .method(HttpMethod.PUT)
      .consumes("application/json")
      .produces("application/json")
      .handler(this);

    router.route()
      .path("/sessions/:sessionId/:path")
      .method(HttpMethod.DELETE)
      .produces("application/json")
      .handler(this);
  }

  @Override
  public void handle(RoutingContext context) {
    final long sessionId = Long.valueOf(context.pathParam("sessionId"));

    final String path = context.pathParam("path");
    final Map<String, String> headers = new HashMap<>();
    for (Map.Entry<String, String> entry : context.request().headers().entries()) {
      headers.put(entry.getKey(), entry.getValue());
    }

    final int code;
    switch (context.request().method()) {
      case POST:
        code = 201;
        break;
      case PUT:
        code = 200;
        break;
      case DELETE:
        code = 202;
        break;
      default:
        code = 200;
        break;
    }

    HttpCommand command = new HttpCommand(path, context.request().method().name(), headers, context.getBodyAsString());
    connection.onCommand(new CommandRequest.Builder()
      .withSession(sessionId)
      .withCommand(command)
      .build(), new CommandResponse.Builder())
      .whenComplete((response, error) -> {
        if (error == null) {
          succeed(context, Json.encode(response.result()), code);
        } else {
          fail(context, error, 500);
        }
      });
  }
}
