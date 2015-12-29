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
 * Connection strategies test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ConnectionStrategiesTest {

  /**
   * Tests the ONCE connection strategy.
   */
  public void testConnectOnceStrategy() throws Throwable {
    ConnectionStrategy strategy = ConnectionStrategies.ONCE;
    ConnectionStrategy.Attempt attempt = mock(ConnectionStrategy.Attempt.class);
    strategy.attemptFailed(attempt);
    verify(attempt).fail();
  }

  /**
   * Tests the exponential backoff strategy.
   */
  public void testConnectExponentialBackoffStrategy() throws Throwable {
    ConnectionStrategy strategy = ConnectionStrategies.EXPONENTIAL_BACKOFF;
    ConnectionStrategy.Attempt attempt;
    attempt = mock(ConnectionStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(1);
    strategy.attemptFailed(attempt);
    verify(attempt).retry(Duration.ofSeconds(2));
    attempt = mock(ConnectionStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(2);
    strategy.attemptFailed(attempt);
    verify(attempt).retry(Duration.ofSeconds(4));
    attempt = mock(ConnectionStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(3);
    strategy.attemptFailed(attempt);
    verify(attempt).retry(Duration.ofSeconds(8));
    attempt = mock(ConnectionStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(4);
    strategy.attemptFailed(attempt);
    verify(attempt).retry(Duration.ofSeconds(16));
    attempt = mock(ConnectionStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(5);
    strategy.attemptFailed(attempt);
    verify(attempt).retry(Duration.ofSeconds(32));
  }

  /**
   * Tests the fibonacci backoff strategy.
   */
  public void testConnectFibonacciBackoffStrategy() throws Throwable {
    ConnectionStrategy strategy = ConnectionStrategies.FIBONACCI_BACKOFF;
    ConnectionStrategy.Attempt attempt;
    attempt = mock(ConnectionStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(1);
    strategy.attemptFailed(attempt);
    verify(attempt).retry(Duration.ofSeconds(1));
    attempt = mock(ConnectionStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(2);
    strategy.attemptFailed(attempt);
    verify(attempt).retry(Duration.ofSeconds(1));
    attempt = mock(ConnectionStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(3);
    strategy.attemptFailed(attempt);
    verify(attempt).retry(Duration.ofSeconds(2));
    attempt = mock(ConnectionStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(4);
    strategy.attemptFailed(attempt);
    verify(attempt).retry(Duration.ofSeconds(3));
    attempt = mock(ConnectionStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(5);
    strategy.attemptFailed(attempt);
    verify(attempt).retry(Duration.ofSeconds(5));
    attempt = mock(ConnectionStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(6);
    strategy.attemptFailed(attempt);
    verify(attempt).retry(Duration.ofSeconds(8));
    attempt = mock(ConnectionStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(7);
    strategy.attemptFailed(attempt);
    verify(attempt).retry(Duration.ofSeconds(13));
    attempt = mock(ConnectionStrategy.Attempt.class);
    when(attempt.attempt()).thenReturn(8);
    strategy.attemptFailed(attempt);
    verify(attempt).retry(Duration.ofSeconds(13));
  }

}
