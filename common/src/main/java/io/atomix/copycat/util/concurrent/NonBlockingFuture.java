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
package io.atomix.copycat.util.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A future that can't be blocked.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NonBlockingFuture<T> extends CompletableFuture<T> {
  @Override
  public T join() {
    throw new UnsupportedOperationException("cannot block the thread");
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    throw new UnsupportedOperationException("cannot block the thread");
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    throw new UnsupportedOperationException("cannot block the thread");
  }
}
