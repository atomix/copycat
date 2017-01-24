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
import io.atomix.copycat.protocol.http.response.HttpErrorResponse;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

/**
 * Request handler.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class RequestHandler implements Handler<RoutingContext> {

  /**
   * Request handler factory.
   */
  public interface Factory {
    RequestHandler create(HttpServerConnection connection);
  }

  protected final HttpServerConnection connection;

  public RequestHandler(HttpServerConnection connection) {
    this.connection = connection;
  }

  /**
   * Registers the handler with the given router.
   *
   * @param router The router with which to register the handler.
   */
  public abstract void register(Router router);

  /**
   * Handles a successful response.
   */
  protected void succeed(RoutingContext context, int code) {
    context.response().setStatusCode(code);
    context.response().end();
  }

  /**
   * Handles a successful response.
   */
  protected void succeed(RoutingContext context, Object response, int code) {
    context.response().putHeader("content-type", "application/json");
    context.response().setStatusCode(code);
    context.response().end(Json.encode(response));
  }

  /**
   * Fails the given request.
   */
  protected void fail(RoutingContext context, Throwable error, int code) {
    HttpErrorResponse response = new HttpErrorResponse();
    response.setError(error.getMessage());
    context.response().putHeader("content-type", "application/json");
    context.response().setStatusCode(code);
    try {
      context.response().end(Json.encode(response));
    } catch (Exception e) {
      context.response().setStatusCode(500);
      context.response().end();
    }
  }

}
