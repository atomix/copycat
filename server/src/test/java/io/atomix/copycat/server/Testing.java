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
package io.atomix.copycat.server;

import java.util.stream.Stream;

/**
 * Test utilities.
 */
public final class Testing {
  static class UncheckedException extends RuntimeException{
    UncheckedException(Throwable cause) {
      super(cause);
    }
  }
  
  @FunctionalInterface
  public interface ThrowableRunnable {
    void run() throws Throwable;
  }

  public interface TConsumer1<A> {
    void accept(A a) throws Throwable;
  }

  public static <A> void withArgs(A[] a, TConsumer1<A> consumer) throws Throwable {
    try {
      Stream.of(a).forEach(arg -> uncheck(() -> consumer.accept(arg)));
    } catch (UncheckedException e) {
      throw e.getCause();
    }
  }

  public static void uncheck(ThrowableRunnable runnable) {
    try {
      runnable.run();
    } catch (Throwable e) {
      throw new UncheckedException(e);
    }
  }
}
