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
package io.atomix.copycat.client;

import java.time.Duration;

/**
 * Strategies for resubmitting failed client operations to the cluster.
 * <p>
 * This enum provides basic {@link RetryStrategy} implementations for dictating how operations
 * submitted by a {@link CopycatClient} are retried in the event of a failure. Retry strategies
 * can be configured via the client {@link CopycatClient.Builder Builder}.
 * <p>
 * <pre>
 *   {@code
 *   CopycatClient client = CopycatClient.builder(members)
 *     .withTransport(new NettyTransport())
 *     .withRetryStrategy(RetryStrategies.EXPONENTIAL_BACKOFF)
 *     .build();
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public enum RetryStrategies implements RetryStrategy {

  /**
   * Fails all attempts.
   */
  FAIL {
    @Override
    public void attemptFailed(Attempt attempt, Throwable cause) {
      attempt.fail(cause);
    }
  },

  /**
   * Retries attempts immediately.
   */
  RETRY {
    @Override
    public void attemptFailed(Attempt attempt, Throwable cause) {
      attempt.retry();
    }
  },

  /**
   * Retries attempts using exponential backoff up to a one minute retry interval.
   */
  EXPONENTIAL_BACKOFF {
    @Override
    public void attemptFailed(Attempt attempt, Throwable cause) {
      attempt.retry(Duration.ofSeconds(Math.min(Math.round(Math.pow(2, attempt.attempt())), 5)));
    }
  },

  /**
   * Retries attempts using fibonacci sequence backoff.
   */
  FIBONACCI_BACKOFF {
    private final int[] FIBONACCI = new int[]{1, 1, 2, 3, 5};

    @Override
    public void attemptFailed(Attempt attempt, Throwable cause) {
      attempt.retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt.attempt()-1, FIBONACCI.length-1)]));
    }
  }

}
