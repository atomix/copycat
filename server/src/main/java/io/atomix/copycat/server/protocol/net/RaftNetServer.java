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

import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.server.protocol.RaftProtocolServer;
import io.atomix.copycat.server.protocol.RaftProtocolServerConnection;
import io.vertx.core.net.NetServer;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Raft TCP protocol server.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class RaftNetServer implements RaftProtocolServer {
  private final NetServer server;
  private final KryoPool kryoPool;

  public RaftNetServer(NetServer server, KryoFactory kryoFactory) {
    this.server = server;
    this.kryoPool = new KryoPool.Builder(kryoFactory).softReferences().build();
  }

  @Override
  public CompletableFuture<Void> listen(Address address, Consumer<RaftProtocolServerConnection> listener) {
    server.connectHandler(socket -> {
      listener.accept(new RaftNetServerConnection(socket, kryoPool));
    });

    CompletableFuture<Void> future = new CompletableFuture<>();
    server.listen(address.port(), address.host(), result -> {
      if (result.succeeded()) {
        future.complete(null);
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
