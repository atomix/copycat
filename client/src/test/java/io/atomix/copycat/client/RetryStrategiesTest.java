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

import org.testng.annotations.Test;

import java.time.Duration;

import static org.mockito.Mockito.*;

/**
 * Retries strategies tests.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class RetryStrategiesTest {

  /**
   * Tests the fail retry strategy.
   * @throws Throwable
   */
  public void testRetryFailStrategy() throws Throwable {
    RetryStrategy strategy = RetryStrategies.FAIL;
    RetryStrategy.Attempt attempt = mock(RetryStrategy.Attempt.class);
    strategy.attemptFailed(attempt, new Exception());
    verify(attempt).fail(any());
  }

  /**
   * Tests the retry strategy.
   */
  public void testRetryStrategy() throws Throwable {
    RetryStrategy strategy = RetryStrategies.RETRY;
    RetryStrategy.Attempt attempt = mock(RetryStrategy.Attempt.class);
    strategy.attemptFailed(attempt, new Exception());
    verify(attempt).retry();
  }

  /**
   * Tests the exponential backoff strategy.
   */
  public void testRetryExponentialBackoffStrategy() throws Throwable {
    RetryStrategy strategy = RetryStrategies.EXPONENTIAL_BACKOFF;
    RetryStrategy.Attempt attempt;
    attempt = mock(RetryStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(1);
    strategy.attemptFailed(attempt, new Exception());
    verify(attempt).retry(Duration.ofSeconds(2));
    attempt = mock(RetryStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(2);
    strategy.attemptFailed(attempt, new Exception());
    verify(attempt).retry(Duration.ofSeconds(4));
    attempt = mock(RetryStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(3);
    strategy.attemptFailed(attempt, new Exception());
    verify(attempt).retry(Duration.ofSeconds(5));
    attempt = mock(RetryStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(4);
    strategy.attemptFailed(attempt, new Exception());
    verify(attempt).retry(Duration.ofSeconds(5));
  }

  /**
   * Tests the fibonacci backoff strategy.
   */
  public void testRetryFibonacciBackoffStrategy() throws Throwable {
    RetryStrategy strategy = RetryStrategies.FIBONACCI_BACKOFF;
    RetryStrategy.Attempt attempt;
    attempt = mock(RetryStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(1);
    strategy.attemptFailed(attempt, new Exception());
    verify(attempt).retry(Duration.ofSeconds(1));
    attempt = mock(RetryStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(2);
    strategy.attemptFailed(attempt, new Exception());
    verify(attempt).retry(Duration.ofSeconds(1));
    attempt = mock(RetryStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(3);
    strategy.attemptFailed(attempt, new Exception());
    verify(attempt).retry(Duration.ofSeconds(2));
    attempt = mock(RetryStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(4);
    strategy.attemptFailed(attempt, new Exception());
    verify(attempt).retry(Duration.ofSeconds(3));
    attempt = mock(RetryStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(5);
    strategy.attemptFailed(attempt, new Exception());
    verify(attempt).retry(Duration.ofSeconds(5));
    attempt = mock(RetryStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(6);
    strategy.attemptFailed(attempt, new Exception());
    verify(attempt).retry(Duration.ofSeconds(5));
  }

}
