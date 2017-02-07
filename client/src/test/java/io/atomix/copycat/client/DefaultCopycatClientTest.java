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
package io.atomix.copycat.client;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Client;
import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.Transport;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.*;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Copycat client test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class DefaultCopycatClientTest {
  private static final Address LEADER = new Address("localhost", 5000);
  private static final Collection<Address> MEMBERS = Arrays.asList(
    new Address("localhost", 5000),
    new Address("localhost", 5001),
    new Address("localhost", 5002)
  );

  /**
   * Tests calling the recovery strategy when a command fails due to UnknownSessionException.
   */
  public void testCommandSessionRecovery() throws Throwable {
    Connection connection = mock(Connection.class);
    when(connection.close()).thenReturn(CompletableFuture.completedFuture(null));

    Client client = mock(Client.class);
    when(client.connect(any())).thenReturn(CompletableFuture.completedFuture(connection));

    Transport transport = mock(Transport.class);
    when(transport.client()).thenReturn(client);

    // Handle connect requests.
    when(connection.send(isA(ConnectRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(ConnectResponse.builder()
        .withStatus(Response.Status.OK)
        .withLeader(LEADER)
        .withMembers(MEMBERS)
        .build()));

    // Handle register requests.
    AtomicLong sessionId = new AtomicLong();
    when(connection.send(isA(RegisterRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(RegisterResponse.builder()
        .withStatus(Response.Status.OK)
        .withSession(sessionId.incrementAndGet())
        .withTimeout(5000)
        .withLeader(LEADER)
        .withMembers(MEMBERS)
        .build()));

    // Handle keep-alive requests.
    Mockito.when(connection.send(isA(KeepAliveRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(KeepAliveResponse.builder()
        .withStatus(Response.Status.OK)
        .withLeader(LEADER)
        .withMembers(MEMBERS)
        .build()));

    // Fail command requests.
    Mockito.when(connection.send(isA(CommandRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(CommandResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(CopycatError.Type.UNKNOWN_SESSION_ERROR)
        .build()));

    final CountDownLatch latch = new CountDownLatch(1);
    CopycatClient copycatClient = CopycatClient.builder()
      .withTransport(transport)
      .withRecoveryStrategy(c -> {
        latch.countDown();
      })
      .build();

    copycatClient.connect(MEMBERS).join();

    copycatClient.submit(new TestCommand());

    latch.await();
  }

  /**
   * Tests calling the recovery strategy when a query fails due to UnknownSessionException.
   */
  public void testQuerySessionRecovery() throws Throwable {
    Connection connection = mock(Connection.class);
    when(connection.close()).thenReturn(CompletableFuture.completedFuture(null));

    Client client = mock(Client.class);
    when(client.connect(any())).thenReturn(CompletableFuture.completedFuture(connection));

    Transport transport = mock(Transport.class);
    when(transport.client()).thenReturn(client);

    // Handle connect requests.
    when(connection.send(isA(ConnectRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(ConnectResponse.builder()
        .withStatus(Response.Status.OK)
        .withLeader(LEADER)
        .withMembers(MEMBERS)
        .build()));

    // Handle register requests.
    AtomicLong sessionId = new AtomicLong();
    when(connection.send(isA(RegisterRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(RegisterResponse.builder()
        .withStatus(Response.Status.OK)
        .withSession(sessionId.incrementAndGet())
        .withTimeout(5000)
        .withLeader(LEADER)
        .withMembers(MEMBERS)
        .build()));

    // Handle keep-alive requests.
    Mockito.when(connection.send(isA(KeepAliveRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(KeepAliveResponse.builder()
        .withStatus(Response.Status.OK)
        .withLeader(LEADER)
        .withMembers(MEMBERS)
        .build()));

    // Fail query requests.
    Mockito.when(connection.send(isA(QueryRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(QueryResponse.builder()
        .withStatus(Response.Status.ERROR)
        .withError(CopycatError.Type.UNKNOWN_SESSION_ERROR)
        .build()));

    final CountDownLatch latch = new CountDownLatch(1);
    CopycatClient copycatClient = CopycatClient.builder()
      .withTransport(transport)
      .withRecoveryStrategy(c -> {
        latch.countDown();
      })
      .build();

    copycatClient.connect(MEMBERS).join();

    copycatClient.submit(new TestQuery());

    latch.await();
  }

  /**
   * Test command.
   */
  private static class TestCommand implements Command<String> {
  }

  /**
   * Test query.
   */
  private static class TestQuery implements Query<String> {
  }

}
