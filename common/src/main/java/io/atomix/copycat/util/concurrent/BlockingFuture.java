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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link java.util.concurrent.CompletableFuture} implementation that aids in detecting blocked threads.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class BlockingFuture<T> extends ComposableFuture<T> {

  @Override
  public T get() throws InterruptedException, ExecutionException {
    ThreadContext context = ThreadContext.currentContext();
    if (context != null) {
      context.block();
      try {
        return super.get();
      } finally {
        context.unblock();
      }
    }
    return super.get();
  }

  @Override
  public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
    ThreadContext context = ThreadContext.currentContext();
    if (context != null) {
      context.block();
      try {
        return super.get(timeout, unit);
      } finally {
        context.unblock();
      }
    }
    return super.get(timeout, unit);
  }

  @Override
  public T join() {
    ThreadContext context = ThreadContext.currentContext();
    if (context != null) {
      context.block();
      try {
        return super.join();
      } finally {
        context.unblock();
      }
    }
    return super.join();
  }
}
