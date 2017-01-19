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

import org.slf4j.Logger;

import java.util.concurrent.RejectedExecutionException;

/**
 * Runnable utilities.
 */
final class Runnables {
  private Runnables() {
  }

  /**
   * Returns a wrapped runnable that logs and rethrows uncaught exceptions.
   */
  static Runnable logFailure(final Runnable runnable, Logger logger) {
    return () -> {
      try {
        runnable.run();
      } catch (Throwable t) {
        if (!(t instanceof RejectedExecutionException)) {
          logger.error("An uncaught exception occurred", t);
        }
        throw t;
      }
    };
  }
}
