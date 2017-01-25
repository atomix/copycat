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
import io.atomix.copycat.server.protocol.RaftProtocolClient;
import io.atomix.copycat.server.protocol.RaftProtocolClientConnection;
import io.vertx.core.net.NetClient;

import java.util.concurrent.CompletableFuture;

/**
 * Raft TCP protocol client.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class RaftNetClient implements RaftProtocolClient {
  private final NetClient client;
  private final KryoPool kryoPool;

  public RaftNetClient(NetClient client, KryoFactory kryoFactory) {
    this.client = client;
    this.kryoPool = new KryoPool.Builder(kryoFactory).softReferences().build();
  }

  @Override
  public CompletableFuture<RaftProtocolClientConnection> connect(Address address) {
    CompletableFuture<RaftProtocolClientConnection> future = new CompletableFuture<>();
    client.connect(address.port(), address.host(), result -> {
      if (result.succeeded()) {
        future.complete(new RaftNetClientConnection(result.result(), kryoPool));
      } else {
        future.completeExceptionally(result.cause());
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    client.close();
    return CompletableFuture.completedFuture(null);
  }
}
