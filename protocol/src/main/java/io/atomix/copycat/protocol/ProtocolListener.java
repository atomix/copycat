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

import io.atomix.copycat.protocol.request.ProtocolRequest;
import io.atomix.copycat.protocol.response.ProtocolResponse;

import java.util.concurrent.CompletableFuture;

/**
 * Protocol listener.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@FunctionalInterface
public interface ProtocolListener<T extends ProtocolRequest, U extends ProtocolResponse.Builder<U, V>, V extends ProtocolResponse> {

  /**
   * Called when a request is received.
   *
   * @param request The request that was received.
   * @param builder The protocol response builder.
   * @return A completable future to be completed with the response.
   */
  CompletableFuture<V> onRequest(T request, U builder);

}
