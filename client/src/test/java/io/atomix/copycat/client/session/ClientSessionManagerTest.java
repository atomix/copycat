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

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.ServerSelectionStrategies;
import io.atomix.copycat.client.request.RegisterRequest;
import io.atomix.copycat.client.request.UnregisterRequest;
import io.atomix.copycat.client.response.RegisterResponse;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.client.response.UnregisterResponse;
import io.atomix.copycat.client.util.AddressSelector;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

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
    Connection connection = mock(Connection.class);
    when(connection.send(any(RegisterRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(RegisterResponse.builder()
        .withSession(1)
        .withLeader(new Address("localhost", 5000))
        .withMembers(Arrays.asList(
          new Address("localhost", 5000),
          new Address("localhost", 5001),
          new Address("localhost", 5002)
        ))
        .withTimeout(1000)
        .build()));

    ClientSessionState state = new ClientSessionState(UUID.randomUUID());
    ThreadContext context = mock(ThreadContext.class);
    Executor executor = new MockExecutor();
    when(context.executor()).thenReturn(executor);

    AddressSelector selector = new AddressSelector(Collections.singletonList(new Address("localhost", 5000)), ServerSelectionStrategies.ANY);
    ClientSessionManager manager = new ClientSessionManager(connection, selector, state, context, ConnectionStrategies.EXPONENTIAL_BACKOFF);
    manager.open().join();

    assertEquals(state.getSessionId(), 1);
    assertEquals(state.getState(), Session.State.OPEN);
    assertEquals(selector.leader(), new Address("localhost", 5000));
    assertTrue(selector.servers().contains(new Address("localhost", 5000)));
    assertTrue(selector.servers().contains(new Address("localhost", 5001)));
    assertTrue(selector.servers().contains(new Address("localhost", 5002)));

    when(connection.send(any(UnregisterRequest.class)))
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
