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

import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.protocol.error.QueryException;
import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.atomix.copycat.protocol.ProtocolRequestFactory;
import io.atomix.copycat.protocol.response.CommandResponse;
import io.atomix.copycat.protocol.response.QueryResponse;
import io.atomix.copycat.protocol.websocket.response.WebSocketCommandResponse;
import io.atomix.copycat.protocol.websocket.response.WebSocketQueryResponse;
import io.atomix.copycat.protocol.websocket.response.WebSocketResponse;
import io.atomix.copycat.session.Session;
import io.atomix.copycat.util.concurrent.ThreadContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Client session submitter test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ClientSessionSubmitterTest {

  /**
   * Tests submitting a command to the cluster.
   */
  @SuppressWarnings("unchecked")
  public void testSubmitCommand() throws Throwable {
    ProtocolClientConnection connection = mock(ProtocolClientConnection.class);
    when(connection.command(any(ProtocolRequestFactory.class)))
      .thenReturn(CompletableFuture.completedFuture(new WebSocketCommandResponse.Builder(1)
        .withStatus(WebSocketResponse.Status.OK)
        .withIndex(10)
        .withResult("Hello world!")
        .build()));

    ClientSessionState state = new ClientSessionState(UUID.randomUUID().toString())
      .setSessionId(1)
      .setState(Session.State.OPEN);

    Executor executor = new MockExecutor();
    ThreadContext context = mock(ThreadContext.class);
    when(context.executor()).thenReturn(executor);

    ClientSessionSubmitter submitter = new ClientSessionSubmitter(connection, state, new ClientSequencer(state), context);
    assertEquals(submitter.submit(new TestCommand()).get(), "Hello world!");
    assertEquals(state.getCommandRequest(), 1);
    assertEquals(state.getCommandResponse(), 1);
    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Test resequencing a command response.
   */
  @SuppressWarnings("unchecked")
  public void testResequenceCommand() throws Throwable {
    CompletableFuture<CommandResponse> future1 = new CompletableFuture<>();
    CompletableFuture<CommandResponse> future2 = new CompletableFuture<>();

    ProtocolClientConnection connection = mock(ProtocolClientConnection.class);
    Mockito.when(connection.command(any(ProtocolRequestFactory.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    ClientSessionState state = new ClientSessionState(UUID.randomUUID().toString())
      .setSessionId(1)
      .setState(Session.State.OPEN);

    Executor executor = new MockExecutor();
    ThreadContext context = mock(ThreadContext.class);
    when(context.executor()).thenReturn(executor);

    ClientSessionSubmitter submitter = new ClientSessionSubmitter(connection, state, new ClientSequencer(state), context);

    CompletableFuture<String> result1 = submitter.submit(new TestCommand());
    CompletableFuture<String> result2 = submitter.submit(new TestCommand());

    future2.complete(new WebSocketCommandResponse.Builder(2)
      .withStatus(WebSocketResponse.Status.OK)
      .withIndex(10)
      .withResult("Hello world again!")
      .build());

    assertEquals(state.getCommandRequest(), 2);
    assertEquals(state.getCommandResponse(), 0);
    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.complete(new WebSocketCommandResponse.Builder(3)
      .withStatus(WebSocketResponse.Status.OK)
      .withIndex(9)
      .withResult("Hello world!")
      .build());

    assertTrue(result1.isDone());
    assertEquals(result1.get(), "Hello world!");
    assertTrue(result2.isDone());
    assertEquals(result2.get(), "Hello world again!");

    assertEquals(state.getCommandRequest(), 2);
    assertEquals(state.getCommandResponse(), 2);
    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Tests submitting a query to the cluster.
   */
  @SuppressWarnings("unchecked")
  public void testSubmitQuery() throws Throwable {
    ProtocolClientConnection connection = mock(ProtocolClientConnection.class);
    when(connection.query(any(ProtocolRequestFactory.class)))
      .thenReturn(CompletableFuture.completedFuture(new WebSocketQueryResponse.Builder(1)
        .withStatus(WebSocketResponse.Status.OK)
        .withIndex(10)
        .withResult("Hello world!")
        .build()));

    ClientSessionState state = new ClientSessionState(UUID.randomUUID().toString())
      .setSessionId(1)
      .setState(Session.State.OPEN);

    Executor executor = new MockExecutor();
    ThreadContext context = mock(ThreadContext.class);
    when(context.executor()).thenReturn(executor);

    ClientSessionSubmitter submitter = new ClientSessionSubmitter(connection, state, new ClientSequencer(state), context);
    assertEquals(submitter.submit(new TestQuery()).get(), "Hello world!");
    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Tests resequencing a query response.
   */
  @SuppressWarnings("unchecked")
  public void testResequenceQuery() throws Throwable {
    CompletableFuture<QueryResponse> future1 = new CompletableFuture<>();
    CompletableFuture<QueryResponse> future2 = new CompletableFuture<>();

    ProtocolClientConnection connection = mock(ProtocolClientConnection.class);
    Mockito.when(connection.query(any(ProtocolRequestFactory.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    ClientSessionState state = new ClientSessionState(UUID.randomUUID().toString())
      .setSessionId(1)
      .setState(Session.State.OPEN);

    Executor executor = new MockExecutor();
    ThreadContext context = mock(ThreadContext.class);
    when(context.executor()).thenReturn(executor);

    ClientSessionSubmitter submitter = new ClientSessionSubmitter(connection, state, new ClientSequencer(state), context);

    CompletableFuture<String> result1 = submitter.submit(new TestQuery());
    CompletableFuture<String> result2 = submitter.submit(new TestQuery());

    future2.complete(new WebSocketQueryResponse.Builder(1)
      .withStatus(WebSocketResponse.Status.OK)
      .withIndex(10)
      .withResult("Hello world again!")
      .build());

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.complete(new WebSocketQueryResponse.Builder(2)
      .withStatus(WebSocketResponse.Status.OK)
      .withIndex(9)
      .withResult("Hello world!")
      .build());

    assertTrue(result1.isDone());
    assertEquals(result1.get(), "Hello world!");
    assertTrue(result2.isDone());
    assertEquals(result2.get(), "Hello world again!");

    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Tests skipping over a failed query attempt.
   */
  @SuppressWarnings("unchecked")
  public void testSkippingOverFailedQuery() throws Throwable {
    CompletableFuture<QueryResponse> future1 = new CompletableFuture<>();
    CompletableFuture<QueryResponse> future2 = new CompletableFuture<>();

    ProtocolClientConnection connection = mock(ProtocolClientConnection.class);
    Mockito.when(connection.query(any(ProtocolRequestFactory.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    ClientSessionState state = new ClientSessionState(UUID.randomUUID().toString())
      .setSessionId(1)
      .setState(Session.State.OPEN);

    Executor executor = new MockExecutor();
    ThreadContext context = mock(ThreadContext.class);
    when(context.executor()).thenReturn(executor);

    ClientSessionSubmitter submitter = new ClientSessionSubmitter(connection, state, new ClientSequencer(state), context);

    CompletableFuture<String> result1 = submitter.submit(new TestQuery());
    CompletableFuture<String> result2 = submitter.submit(new TestQuery());

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.completeExceptionally(new QueryException("failure"));
    future2.complete(new WebSocketQueryResponse.Builder(1)
        .withStatus(WebSocketResponse.Status.OK)
        .withIndex(10)
        .withResult("Hello world!")
        .build());

    assertTrue(result1.isCompletedExceptionally());
    assertTrue(result2.isDone());
    assertEquals(result2.get(), "Hello world!");

    assertEquals(state.getResponseIndex(), 10);
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
