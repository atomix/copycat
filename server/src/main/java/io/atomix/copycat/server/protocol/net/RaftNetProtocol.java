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
package io.atomix.copycat.server.protocol.net;

import io.atomix.copycat.server.protocol.RaftProtocol;
import io.atomix.copycat.server.protocol.RaftProtocolClient;
import io.atomix.copycat.server.protocol.RaftProtocolServer;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletableFuture;

/**
 * Raft TCP protocol.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class RaftNetProtocol implements RaftProtocol {
  private final Vertx vertx;

  public RaftNetProtocol() {
    this(Vertx.vertx());
  }

  public RaftNetProtocol(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public RaftProtocolServer createServer() {
    return new RaftNetServer(vertx.createNetServer());
  }

  @Override
  public RaftProtocolClient createClient() {
    return new RaftNetClient(vertx.createNetClient());
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
