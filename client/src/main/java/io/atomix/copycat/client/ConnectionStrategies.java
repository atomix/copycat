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
 * Basic connection strategies.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public enum ConnectionStrategies implements ConnectionStrategy {

  /**
   * Attempts to connect to the cluster one time and fails.
   */
  ONCE {
    @Override
    public void attemptFailed(Attempt attempt) {
      attempt.fail();
    }
  },

  /**
   * Attempts to connect to the cluster using exponential backoff up to a one minute retry interval.
   */
  BACKOFF {
    @Override
    public void attemptFailed(Attempt attempt) {
      attempt.retry(Duration.ofSeconds(Math.min(attempt.attempt() * attempt.attempt(), 60)));
    }
  }

}
