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
import io.atomix.copycat.protocol.http.request.HttpCreateSessionRequest;
import io.atomix.copycat.protocol.http.response.HttpCreateSessionResponse;
import io.atomix.copycat.protocol.request.RegisterRequest;
import io.atomix.copycat.protocol.response.RegisterResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

/**
 * Create session handler.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class CreateSessionHandler extends RequestHandler {

  /**
   * Create session request handler factory.
   */
  public static class Factory implements RequestHandler.Factory {
    @Override
    public RequestHandler create(HttpServerConnection connection) {
      return new CreateSessionHandler(connection);
    }
  }

  public CreateSessionHandler(HttpServerConnection connection) {
    super(connection);
  }

  @Override
  public void register(Router router) {
    router.route()
      .path("/sessions")
      .method(HttpMethod.POST)
      .consumes("application/json")
      .produces("application/json")
      .handler(this);
  }

  @Override
  public void handle(RoutingContext context) {
    HttpCreateSessionRequest jsonRequest = Json.decodeValue(context.getBodyAsString(), HttpCreateSessionRequest.class);
    connection.onRegister(new RegisterRequest.Builder()
      .withClient(jsonRequest.getName())
      .withTimeout(jsonRequest.getTimeout())
      .build(), new RegisterResponse.Builder()).whenComplete((response, error) -> {
      if (error == null) {
        succeed(context, new HttpCreateSessionResponse()
          .setSession(response.session()), 201);
      } else {
        fail(context, error, 500);
      }
    });
  }
}
