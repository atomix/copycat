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
package io.atomix.copycat.protocol.http;

import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.ProtocolServer;
import io.atomix.copycat.protocol.ProtocolServerConnection;
import io.atomix.copycat.protocol.http.handlers.RequestHandler;
import io.atomix.copycat.protocol.http.handlers.RequestHandlers;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * HTTP protocol server.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class HttpServer implements ProtocolServer {
  protected final Vertx vertx;
  protected final io.vertx.core.http.HttpServer server;
  private final Collection<RequestHandler.Factory> handlers;

  public HttpServer(Vertx vertx, io.vertx.core.http.HttpServer server) {
    this(vertx, server, RequestHandlers.ALL);
  }

  public HttpServer(Vertx vertx, io.vertx.core.http.HttpServer server, Collection<RequestHandler.Factory> handlers) {
    this.vertx = vertx;
    this.server = server;
    this.handlers = handlers;
  }

  @Override
  public CompletableFuture<Void> listen(Address address, Consumer<ProtocolServerConnection> listener) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    server.listen(address.port(), address.host(), result -> {
      if (result.succeeded()) {
        future.complete(null);
        listener.accept(new HttpServerConnection(Router.router(vertx), handlers));
      } else {
        future.completeExceptionally(result.cause());
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    server.close(result -> {
      if (result.succeeded()) {
        future.complete(null);
      } else {
        future.completeExceptionally(result.cause());
      }
    });
    return future;
  }
}
