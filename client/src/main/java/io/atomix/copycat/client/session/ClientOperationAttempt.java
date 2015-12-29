/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.copycat.client.session;

import io.atomix.copycat.client.request.OperationRequest;
import io.atomix.copycat.client.response.OperationResponse;
import io.atomix.copycat.client.RetryStrategy;

import java.util.concurrent.CompletableFuture;

/**
 * Client operation attempt.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class ClientOperationAttempt<T extends OperationRequest<T>, U extends OperationResponse<U>, V> implements RetryStrategy.Attempt {
  private final T request;
  private final CompletableFuture<V> future;

  public ClientOperationAttempt(T request, CompletableFuture<V> future) {
    this.request = request;
    this.future = future;
  }

  /**
   * Returns the attempt request.
   *
   * @return The attempt request.
   */
  public T request() {
    return request;
  }

}
