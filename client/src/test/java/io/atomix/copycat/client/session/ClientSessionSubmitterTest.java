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

import io.atomix.copycat.ConsistencyLevel;
import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.atomix.copycat.protocol.error.QueryException;
import io.atomix.copycat.protocol.request.CommandRequest;
import io.atomix.copycat.protocol.request.QueryRequest;
import io.atomix.copycat.protocol.response.CommandResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.response.QueryResponse;
import io.atomix.copycat.util.buffer.BufferInput;
import io.atomix.copycat.util.buffer.HeapBuffer;
import io.atomix.copycat.util.concurrent.ThreadContext;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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
    when(connection.command(any(CommandRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(CommandResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withIndex(10)
        .withResult("Hello world!".getBytes())
        .build()));

    ClientSessionState state = new ClientSessionState(UUID.randomUUID().toString())
      .setSessionId(1)
      .setState(ClientSession.State.OPEN);

    ThreadContext context = mock(ThreadContext.class);
    doAnswer((a) -> {
      ((Runnable) a.getArguments()[0]).run();
      return null;
    }).when(context).execute(any(Runnable.class));

    ClientSessionSubmitter submitter = new ClientSessionSubmitter(connection, state, new ClientSequencer(state), context);
    assertEquals(submitter.submitCommand(HeapBuffer.allocate().flip()).get().readBytes(), "Hello world!".getBytes());
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
    Mockito.when(connection.command(any(CommandRequest.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    ClientSessionState state = new ClientSessionState(UUID.randomUUID().toString())
      .setSessionId(1)
      .setState(ClientSession.State.OPEN);

    ThreadContext context = mock(ThreadContext.class);
    doAnswer((a) -> {
      ((Runnable) a.getArguments()[0]).run();
      return null;
    }).when(context).execute(any(Runnable.class));

    ClientSessionSubmitter submitter = new ClientSessionSubmitter(connection, state, new ClientSequencer(state), context);

    CompletableFuture<BufferInput> result1 = submitter.submitCommand(HeapBuffer.allocate().flip());
    CompletableFuture<BufferInput> result2 = submitter.submitCommand(HeapBuffer.allocate().flip());

    future2.complete(CommandResponse.builder()
      .withStatus(ProtocolResponse.Status.OK)
      .withIndex(10)
      .withResult("Hello world again!".getBytes())
      .build());

    assertEquals(state.getCommandRequest(), 2);
    assertEquals(state.getCommandResponse(), 0);
    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.complete(CommandResponse.builder()
      .withStatus(ProtocolResponse.Status.OK)
      .withIndex(9)
      .withResult("Hello world!".getBytes())
      .build());

    assertTrue(result1.isDone());
    assertEquals(result1.get().readBytes(), "Hello world!".getBytes());
    assertTrue(result2.isDone());
    assertEquals(result2.get().readBytes(), "Hello world again!".getBytes());

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
    when(connection.query(any(QueryRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(QueryResponse.builder()
        .withStatus(ProtocolResponse.Status.OK)
        .withIndex(10)
        .withResult("Hello world!".getBytes())
        .build()));

    ClientSessionState state = new ClientSessionState(UUID.randomUUID().toString())
      .setSessionId(1)
      .setState(ClientSession.State.OPEN);

    ThreadContext context = mock(ThreadContext.class);
    doAnswer((a) -> {
      ((Runnable) a.getArguments()[0]).run();
      return null;
    }).when(context).execute(any(Runnable.class));

    ClientSessionSubmitter submitter = new ClientSessionSubmitter(connection, state, new ClientSequencer(state), context);
    assertEquals(submitter.submitQuery(HeapBuffer.allocate().flip(), ConsistencyLevel.LINEARIZABLE).get().readBytes(), "Hello world!".getBytes());
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
    Mockito.when(connection.query(any(QueryRequest.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    ClientSessionState state = new ClientSessionState(UUID.randomUUID().toString())
      .setSessionId(1)
      .setState(ClientSession.State.OPEN);

    ThreadContext context = mock(ThreadContext.class);
    doAnswer((a) -> {
      ((Runnable) a.getArguments()[0]).run();
      return null;
    }).when(context).execute(any(Runnable.class));

    ClientSessionSubmitter submitter = new ClientSessionSubmitter(connection, state, new ClientSequencer(state), context);

    CompletableFuture<BufferInput> result1 = submitter.submitQuery(HeapBuffer.allocate().flip(), ConsistencyLevel.LINEARIZABLE);
    CompletableFuture<BufferInput> result2 = submitter.submitQuery(HeapBuffer.allocate().flip(), ConsistencyLevel.LINEARIZABLE);

    future2.complete(QueryResponse.builder()
      .withStatus(ProtocolResponse.Status.OK)
      .withIndex(10)
      .withResult("Hello world again!".getBytes())
      .build());

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.complete(QueryResponse.builder()
      .withStatus(ProtocolResponse.Status.OK)
      .withIndex(9)
      .withResult("Hello world!".getBytes())
      .build());

    assertTrue(result1.isDone());
    assertEquals(result1.get().readBytes(), "Hello world!".getBytes());
    assertTrue(result2.isDone());
    assertEquals(result2.get().readBytes(), "Hello world again!".getBytes());

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
    Mockito.when(connection.query(any(QueryRequest.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    ClientSessionState state = new ClientSessionState(UUID.randomUUID().toString())
      .setSessionId(1)
      .setState(ClientSession.State.OPEN);

    ThreadContext context = mock(ThreadContext.class);
    doAnswer((a) -> {
      ((Runnable) a.getArguments()[0]).run();
      return null;
    }).when(context).execute(any(Runnable.class));

    ClientSessionSubmitter submitter = new ClientSessionSubmitter(connection, state, new ClientSequencer(state), context);

    CompletableFuture<BufferInput> result1 = submitter.submitQuery(HeapBuffer.allocate().flip(), ConsistencyLevel.LINEARIZABLE);
    CompletableFuture<BufferInput> result2 = submitter.submitQuery(HeapBuffer.allocate().flip(), ConsistencyLevel.LINEARIZABLE);

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.completeExceptionally(new QueryException("failure"));
    future2.complete(QueryResponse.builder()
      .withStatus(ProtocolResponse.Status.OK)
      .withIndex(10)
      .withResult("Hello world!".getBytes())
      .build());

    assertTrue(result1.isCompletedExceptionally());
    assertTrue(result2.isDone());
    assertEquals(result2.get().readBytes(), "Hello world!".getBytes());

    assertEquals(state.getResponseIndex(), 10);
  }
}
