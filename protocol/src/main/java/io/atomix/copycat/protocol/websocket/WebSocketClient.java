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
package io.atomix.copycat.protocol.websocket;

import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.ProtocolClient;
import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.vertx.core.http.HttpClient;

import java.util.concurrent.CompletableFuture;

/**
 * Base client.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class WebSocketClient implements ProtocolClient {
  private final HttpClient client;

  public WebSocketClient(HttpClient client) {
    this.client = client;
  }

  @Override
  public CompletableFuture<ProtocolClientConnection> connect(Address address) {
    CompletableFuture<ProtocolClientConnection> future = new CompletableFuture<>();
    client.websocket(address.port(), address.host(), "/socket", socket -> {
      future.complete(new WebSocketClientConnection(socket));
    }, future::completeExceptionally);
    return future;
  }

  @Override
  public CompletableFuture<Void> close() {
    client.close();
    return CompletableFuture.completedFuture(null);
  }
}
