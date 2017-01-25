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

import com.esotericsoftware.kryo.Kryo;
import io.atomix.copycat.server.protocol.RaftProtocol;
import io.atomix.copycat.server.protocol.RaftProtocolClient;
import io.atomix.copycat.server.protocol.RaftProtocolServer;
import io.atomix.copycat.server.storage.Indexed;
import io.atomix.copycat.server.storage.entry.*;
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

  /**
   * Creates a new Kryo serializer.
   */
  private static Kryo createKryo() {
    Kryo kryo = new Kryo();
    kryo.register(Indexed.class, new Indexed.Serializer());
    kryo.register(InitializeEntry.class, new InitializeEntry.Serializer());
    kryo.register(ConnectEntry.class, new ConnectEntry.Serializer());
    kryo.register(RegisterEntry.class, new RegisterEntry.Serializer());
    kryo.register(KeepAliveEntry.class, new KeepAliveEntry.Serializer());
    kryo.register(UnregisterEntry.class, new UnregisterEntry.Serializer());
    kryo.register(CommandEntry.class, new CommandEntry.Serializer());
    kryo.register(QueryEntry.class, new QueryEntry.Serializer());
    return kryo;
  }

  @Override
  public RaftProtocolServer createServer() {
    return new RaftNetServer(vertx.createNetServer(), RaftNetProtocol::createKryo);
  }

  @Override
  public RaftProtocolClient createClient() {
    return new RaftNetClient(vertx.createNetClient(), RaftNetProtocol::createKryo);
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
