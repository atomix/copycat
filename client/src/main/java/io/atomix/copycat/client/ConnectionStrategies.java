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
 * Strategies for handling client connection failures.
 * <p>
 * This enum provides basic {@link ConnectionStrategy} implementations for common use cases.
 * Client connection strategies can be configured when constructing a client via the client
 * {@link io.atomix.copycat.client.CopycatClient.Builder Builder}.
 * <p>
 * <pre>
 *   {@code
 *   CopycatClient client = CopycatClient.builder(members)
 *     .withTransport(new NettyTransport())
 *     .withConnectionStrategy(ConnectionStrategies.EXPONENTIAL_BACKOFF)
 *     .build();
 *   }
 * </pre>
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public enum ConnectionStrategies implements ConnectionStrategy {

  /**
   * Attempts to connect to the cluster one time and fails.
   * <p>
   * This strategy attempts to establish a connection and session with the cluster. In the event that
   * the client cannot connect to or register a session through any server in the cluster the attempt
   * will immediately fail.
   */
  ONCE {
    @Override
    public void attemptFailed(Attempt attempt) {
      attempt.fail();
    }
  },

  /**
   * Attempts to connect to the cluster using exponential backoff up to a one minute retry interval.
   * <p>
   * This strategy guarantees that the client can only be opened successfully and cannot fail registering
   * a session. In the event that the client cannot connect to any node in the cluster or cannot register
   * a session through any node in the cluster, the client will retry using exponential backoff until the
   * client successfully connects to a server and registers a session.
   */
  EXPONENTIAL_BACKOFF {
    @Override
    public void attemptFailed(Attempt attempt) {
      attempt.retry(Duration.ofSeconds(Math.min(Math.round(Math.pow(2, attempt.attempt())), 60)));
    }
  },

  /**
   * Attempts to connect to the cluster using fibonacci sequence backoff.
   * <p>
   * This strategy guarantees that the client can only be opened successfully and cannot fail registering
   * a session. In the event that the client cannot connect to any node in the cluster or cannot register
   * a session through any node in the cluster, the client will retry using fibonacci backoff until the
   * client successfully connects to a server and registers a session.
   */
  FIBONACCI_BACKOFF {
    private final int[] FIBONACCI = new int[]{1, 1, 2, 3, 5, 8, 13};

    @Override
    public void attemptFailed(Attempt attempt) {
      attempt.retry(Duration.ofSeconds(FIBONACCI[Math.min(attempt.attempt()-1, FIBONACCI.length-1)]));
    }
  }

}
