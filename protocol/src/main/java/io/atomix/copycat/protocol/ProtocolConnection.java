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

import io.atomix.copycat.util.concurrent.Listener;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Base protocol connection.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface ProtocolConnection {

  /**
   * Sets an exception listener on the connection.
   * <p>
   * In the event of an exception in the connection, the provided listener's {@link Consumer#accept(Object)} method will
   * be invoked. To unregister the listener, simply {@link Listener#close()} the returned
   * {@link Listener}.
   *
   * @param listener The exception listener.
   * @return The connection.
   * @throws NullPointerException if {@code listener} is null
   */
  Listener<Throwable> onException(Consumer<Throwable> listener);

  /**
   * Sets a close listener on the connection.
   * <p>
   * The provided listener's {@link Consumer#accept(Object)} method will be invoked when the connection is closed. Note
   * that a close event can be triggered via {@link ProtocolConnection#close()} or by the
   * {@link ProtocolClient} or {@link ProtocolServer} that created the connection.
   *
   * @param listener The close listener.
   * @return The connection.
   * @throws NullPointerException if {@code listener} is null
   */
  Listener<ProtocolConnection> onClose(Consumer<ProtocolConnection> listener);

  /**
   * Closes the connection.
   *
   * @return A completable future to be completed once the connection is closed.
   */
  CompletableFuture<Void> close();

}
