/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.copycat.client.session.impl;

import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.error.QueryException;
import io.atomix.copycat.error.UnknownSessionException;
import io.atomix.copycat.protocol.*;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import java.time.Duration;
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
public class CopycatSessionSubmitterTest {

  /**
   * Tests submitting a command to the cluster.
   */
  public void testSubmitCommand() throws Throwable {
    CopycatConnection connection = mock(CopycatLeaderConnection.class);
    when(connection.sendAndReceive(CommandRequest.NAME, any(CommandRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(CommandResponse.builder()
        .withStatus(Response.Status.OK)
        .withIndex(10)
        .withResult("Hello world!")
        .build()));

    CopycatSessionState state = new CopycatSessionState(1, UUID.randomUUID().toString(), "test");
    CopycatSessionManager manager = mock(CopycatSessionManager.class);
    ThreadContext threadContext = new TestContext();

    CopycatSessionSubmitter submitter = new CopycatSessionSubmitter(connection, mock(CopycatSessionConnection.class), state, new CopycatSessionSequencer(state), manager, threadContext);
    assertEquals(submitter.submit(new TestCommand()).get(), "Hello world!");
    assertEquals(state.getCommandRequest(), 1);
    assertEquals(state.getCommandResponse(), 1);
    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Test resequencing a command response.
   */
  public void testResequenceCommand() throws Throwable {
    CompletableFuture<CommandResponse> future1 = new CompletableFuture<>();
    CompletableFuture<CommandResponse> future2 = new CompletableFuture<>();

    CopycatConnection connection = mock(CopycatLeaderConnection.class);
    Mockito.<CompletableFuture<CommandResponse>>when(connection.sendAndReceive(CommandRequest.NAME, any(CommandRequest.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    CopycatSessionState state = new CopycatSessionState(1, UUID.randomUUID().toString(), "test");
    CopycatSessionManager manager = mock(CopycatSessionManager.class);
    ThreadContext threadContext = new TestContext();

    CopycatSessionSubmitter submitter = new CopycatSessionSubmitter(connection, mock(CopycatSessionConnection.class), state, new CopycatSessionSequencer(state), manager, threadContext);

    CompletableFuture<String> result1 = submitter.submit(new TestCommand());
    CompletableFuture<String> result2 = submitter.submit(new TestCommand());

    future2.complete(CommandResponse.builder()
      .withStatus(Response.Status.OK)
      .withIndex(10)
      .withResult("Hello world again!")
      .build());

    assertEquals(state.getCommandRequest(), 2);
    assertEquals(state.getCommandResponse(), 0);
    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.complete(CommandResponse.builder()
      .withStatus(Response.Status.OK)
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
  public void testSubmitQuery() throws Throwable {
    CopycatConnection connection = mock(CopycatSessionConnection.class);
    when(connection.sendAndReceive(QueryRequest.NAME, any(QueryRequest.class)))
      .thenReturn(CompletableFuture.completedFuture(QueryResponse.builder()
        .withStatus(Response.Status.OK)
        .withIndex(10)
        .withResult("Hello world!")
        .build()));

    CopycatSessionState state = new CopycatSessionState(1, UUID.randomUUID().toString(), "test");
    CopycatSessionManager manager = mock(CopycatSessionManager.class);
    ThreadContext threadContext = new TestContext();

    CopycatSessionSubmitter submitter = new CopycatSessionSubmitter(mock(CopycatLeaderConnection.class), connection, state, new CopycatSessionSequencer(state), manager, threadContext);
    assertEquals(submitter.submit(new TestQuery()).get(), "Hello world!");
    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Tests resequencing a query response.
   */
  public void testResequenceQuery() throws Throwable {
    CompletableFuture<QueryResponse> future1 = new CompletableFuture<>();
    CompletableFuture<QueryResponse> future2 = new CompletableFuture<>();

    CopycatConnection connection = mock(CopycatSessionConnection.class);
    Mockito.<CompletableFuture<QueryResponse>>when(connection.sendAndReceive(QueryRequest.NAME, any(QueryRequest.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    CopycatSessionState state = new CopycatSessionState(1, UUID.randomUUID().toString(), "test");
    CopycatSessionManager manager = mock(CopycatSessionManager.class);
    ThreadContext threadContext = new TestContext();

    CopycatSessionSubmitter submitter = new CopycatSessionSubmitter(mock(CopycatLeaderConnection.class), connection, state, new CopycatSessionSequencer(state), manager, threadContext);

    CompletableFuture<String> result1 = submitter.submit(new TestQuery());
    CompletableFuture<String> result2 = submitter.submit(new TestQuery());

    future2.complete(QueryResponse.builder()
      .withStatus(Response.Status.OK)
      .withIndex(10)
      .withResult("Hello world again!")
      .build());

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.complete(QueryResponse.builder()
      .withStatus(Response.Status.OK)
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
  public void testSkippingOverFailedQuery() throws Throwable {
    CompletableFuture<QueryResponse> future1 = new CompletableFuture<>();
    CompletableFuture<QueryResponse> future2 = new CompletableFuture<>();

    CopycatConnection connection = mock(CopycatSessionConnection.class);
    Mockito.<CompletableFuture<QueryResponse>>when(connection.sendAndReceive(QueryRequest.NAME, any(QueryRequest.class)))
      .thenReturn(future1)
      .thenReturn(future2);

    CopycatSessionState state = new CopycatSessionState(1, UUID.randomUUID().toString(), "test");
    CopycatSessionManager manager = mock(CopycatSessionManager.class);
    ThreadContext threadContext = new TestContext();

    CopycatSessionSubmitter submitter = new CopycatSessionSubmitter(mock(CopycatLeaderConnection.class), connection, state, new CopycatSessionSequencer(state), manager, threadContext);

    CompletableFuture<String> result1 = submitter.submit(new TestQuery());
    CompletableFuture<String> result2 = submitter.submit(new TestQuery());

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result1.isDone());
    assertFalse(result2.isDone());

    future1.completeExceptionally(new QueryException("failure"));
    future2.complete(QueryResponse.builder()
      .withStatus(Response.Status.OK)
      .withIndex(10)
      .withResult("Hello world!")
      .build());

    assertTrue(result1.isCompletedExceptionally());
    assertTrue(result2.isDone());
    assertEquals(result2.get(), "Hello world!");

    assertEquals(state.getResponseIndex(), 10);
  }

  /**
   * Tests that the client's session is expired when an UnknownSessionException is received from the cluster.
   */
  public void testExpireSessionOnCommandFailure() throws Throwable {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();

    CopycatConnection connection = mock(CopycatLeaderConnection.class);
    Mockito.<CompletableFuture<QueryResponse>>when(connection.sendAndReceive(QueryRequest.NAME, any(QueryRequest.class)))
      .thenReturn(future);

    CopycatSessionState state = new CopycatSessionState(1, UUID.randomUUID().toString(), "test");
    CopycatSessionManager manager = mock(CopycatSessionManager.class);
    ThreadContext threadContext = new TestContext();

    CopycatSessionSubmitter submitter = new CopycatSessionSubmitter(connection, mock(CopycatSessionConnection.class), state, new CopycatSessionSequencer(state), manager, threadContext);

    CompletableFuture<String> result = submitter.submit(new TestCommand());

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result.isDone());

    future.completeExceptionally(new UnknownSessionException("unknown session"));

    assertTrue(result.isCompletedExceptionally());
  }

  /**
   * Tests that the client's session is expired when an UnknownSessionException is received from the cluster.
   */
  public void testExpireSessionOnQueryFailure() throws Throwable {
    CompletableFuture<QueryResponse> future = new CompletableFuture<>();

    CopycatConnection connection = mock(CopycatSessionConnection.class);
    Mockito.<CompletableFuture<QueryResponse>>when(connection.sendAndReceive(QueryRequest.NAME, any(QueryRequest.class)))
      .thenReturn(future);

    CopycatSessionState state = new CopycatSessionState(1, UUID.randomUUID().toString(), "test");
    CopycatSessionManager manager = mock(CopycatSessionManager.class);
    ThreadContext threadContext = new TestContext();

    CopycatSessionSubmitter submitter = new CopycatSessionSubmitter(mock(CopycatLeaderConnection.class), connection, state, new CopycatSessionSequencer(state), manager, threadContext);

    CompletableFuture<String> result = submitter.submit(new TestQuery());

    assertEquals(state.getResponseIndex(), 1);

    assertFalse(result.isDone());

    future.completeExceptionally(new UnknownSessionException("unknown session"));

    assertTrue(result.isCompletedExceptionally());
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
   * Test thread context.
   */
  private static class TestContext implements ThreadContext {
    @Override
    public Logger logger() {
      return null;
    }

    @Override
    public Serializer serializer() {
      return null;
    }

    @Override
    public boolean isBlocked() {
      return false;
    }

    @Override
    public void block() {

    }

    @Override
    public void unblock() {

    }

    @Override
    public Scheduled schedule(Duration delay, Runnable callback) {
      return null;
    }

    @Override
    public Scheduled schedule(Duration initialDelay, Duration interval, Runnable callback) {
      return null;
    }

    @Override
    public void close() {

    }

    @Override
    public void execute(Runnable command) {
      command.run();
    }
  }

}
