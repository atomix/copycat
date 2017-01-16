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
package io.atomix.copycat.protocol;

import java.util.concurrent.CompletableFuture;

/**
 * Copycat protocol client.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ProtocolClient {

  /**
   * Connects the client to the given address.
   *
   * @param address The address to which to connect.
   * @return A completable future to be completed once the client has been connected.
   * @throws NullPointerException if {@code address} is null
   * @throws IllegalStateException if not called from a Catalyst thread
   */
  CompletableFuture<ProtocolClientConnection> connect(Address address);

  /**
   * Closes the client.
   * <p>
   * Before the client is closed, all {@link ProtocolClientConnection}s opened by the client will be closed
   * and any registered {@link ProtocolClientConnection#closeListener(java.util.function.Consumer)}s will be invoked.
   *
   * @return A completable future to be called once the client is closed.
   * @throws IllegalStateException if not called from a Catalyst thread
   */
  CompletableFuture<Void> close();

}
