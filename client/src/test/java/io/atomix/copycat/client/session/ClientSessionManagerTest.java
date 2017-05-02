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

import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.util.ClientConnection;
import io.atomix.copycat.protocol.*;
import io.atomix.copycat.session.Session;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

/**
 * Client session manager test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ClientSessionManagerTest {

  /**
   * Tests registering a session with a client session manager.
   */
  public void testSessionRegisterUnregister() throws Throwable {
    ClientConnection connection = mock(ClientConnection.class);
    when(connection.reset()).thenReturn(connection);
    when(connection.servers()).thenReturn(Collections.singletonList(new Address("localhost", 5000)));
    when(connection.sendAndReceive(any(RegisterRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(RegisterResponse.builder()
        .withSession(1)
        .withLeader(new Address("localhost", 5000))
        .withMembers(Arrays.asList(
          new Address("localhost", 5000),
          new Address("localhost", 5001),
          new Address("localhost", 5002)
        ))
        .withTimeout(1000)
        .build()))
      .thenReturn(CompletableFuture.completedFuture(KeepAliveResponse.builder()
        .withStatus(Response.Status.OK)
        .withLeader(new Address("localhost", 5000))
        .withMembers(Arrays.asList(
          new Address("localhost", 5000),
          new Address("localhost", 5001),
          new Address("localhost", 5002)
        )).build()));

    ClientSessionState state = new ClientSessionState(UUID.randomUUID().toString());
    ThreadContext context = mock(ThreadContext.class);
    Executor executor = new MockExecutor();
    when(context.executor()).thenReturn(executor);

    ClientSessionManager manager = new ClientSessionManager(connection, state, context, ConnectionStrategies.EXPONENTIAL_BACKOFF, Duration.ZERO);
    manager.open().join();

    assertEquals(state.getSessionId(), 1);
    assertEquals(state.getState(), Session.State.OPEN);

    verify(connection, times(2)).reset(new Address("localhost", 5000), Arrays.asList(
      new Address("localhost", 5000),
      new Address("localhost", 5001),
      new Address("localhost", 5002)
    ));

    when(connection.sendAndReceive(any(UnregisterRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(UnregisterResponse.builder()
        .withStatus(Response.Status.OK)
        .build()));

    manager.close().join();

    assertEquals(state.getState(), Session.State.CLOSED);
  }

  /**
   * Mock executor.
   */
  private static class MockExecutor implements Executor {
    @Override
    public void execute(Runnable command) {
      command.run();
    }
  }

}
