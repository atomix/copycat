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

import io.atomix.copycat.protocol.Protocol;
import io.atomix.copycat.protocol.ProtocolClient;
import io.atomix.copycat.protocol.ProtocolServer;
import io.atomix.copycat.protocol.http.handlers.RequestHandlers;
import io.atomix.copycat.protocol.websocket.WebSocketClient;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletableFuture;

/**
 * HTTP protocol.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class HttpProtocol implements Protocol {
  private final Vertx vertx;

  public HttpProtocol() {
    this(Vertx.vertx());
  }

  public HttpProtocol(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public ProtocolClient createClient() {
    return new WebSocketClient(vertx.createHttpClient());
  }

  @Override
  public ProtocolServer createServer() {
    return new HttpServer(vertx, vertx.createHttpServer());
  }

  @Override
  public CompletableFuture<Void> close() {
    CompletableFuture<Void> future = new CompletableFuture<>();
    vertx.close(result -> {
      if (result.succeeded()) {
        future.complete(null);
      } else {
        future.completeExceptionally(result.cause());
      }
    });
    return future;
  }
}
