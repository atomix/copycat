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

import io.atomix.catalyst.transport.Connection;
import io.atomix.catalyst.transport.MessageHandler;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.protocol.PublishRequest;
import io.atomix.copycat.protocol.PublishResponse;
import io.atomix.copycat.protocol.Response;
import io.atomix.copycat.session.Event;
import io.atomix.copycat.session.Session;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.testng.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Client session listener test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ClientSessionListenerTest {
  private ClientSessionState state;
  private MessageHandler<PublishRequest, PublishResponse> handler;

  /**
   * Creates a client session listener.
   */
  @SuppressWarnings("unchecked")
  private ClientSessionListener createListener() throws Throwable {
    ArgumentCaptor<MessageHandler> captor = ArgumentCaptor.forClass(MessageHandler.class);
    Connection connection = mock(Connection.class);

    state = new ClientSessionState(UUID.randomUUID());
    Executor executor = new MockExecutor();
    ThreadContext context = mock(ThreadContext.class);
    when(context.executor()).thenReturn(executor);

    state.setSessionId(1).setState(Session.State.OPEN);

    ClientSessionListener listener = new ClientSessionListener(connection, state, context);
    verify(connection).handler(any(Class.class), captor.capture());
    handler = captor.getValue();
    return listener;
  }

  /**
   * Tests receiving an event on a session listener.
   */
  public void testSessionReceiveEvent() throws Throwable {
    ClientSessionListener listener = createListener();

    AtomicBoolean received = new AtomicBoolean();
    listener.onEvent("foo", value -> {
      assertEquals(value, "Hello world!");
      received.set(true);
    });

    PublishResponse response;
    response = handler.handle(PublishRequest.builder()
      .withSession(1)
      .withEventIndex(10)
      .withPreviousIndex(1)
      .withEvents(new Event<String>("foo", "Hello world!"))
      .build()).get();

    assertEquals(response.status(), Response.Status.OK);
    assertEquals(response.index(), 10);
    assertEquals(state.getEventIndex(), 10);
    assertEquals(state.getCompleteIndex(), 10);
    assertTrue(received.get());
  }

  /**
   * Tests ignoring an event that was already received.
   */
  public void testSessionIgnoreOldEvent() throws Throwable {
    ClientSessionListener listener = createListener();

    AtomicBoolean received = new AtomicBoolean();
    listener.onEvent("foo", value -> {
      assertEquals(value, "Hello world!");
      received.set(true);
    });

    PublishResponse response;
    response = handler.handle(PublishRequest.builder()
      .withSession(1)
      .withEventIndex(10)
      .withPreviousIndex(1)
      .withEvents(new Event<String>("foo", "Hello world!"))
      .build()).get();

    assertEquals(response.status(), Response.Status.OK);
    assertEquals(response.index(), 10);
    assertEquals(state.getEventIndex(), 10);
    assertEquals(state.getCompleteIndex(), 10);
    assertTrue(received.get());

    received.set(false);
    response = handler.handle(PublishRequest.builder()
      .withSession(1)
      .withEventIndex(10)
      .withPreviousIndex(1)
      .withEvents(new Event<String>("foo", "Hello world!"))
      .build()).get();

    assertEquals(response.status(), Response.Status.ERROR);
    assertEquals(response.index(), 10);
    assertEquals(state.getEventIndex(), 10);
    assertEquals(state.getCompleteIndex(), 10);
    assertFalse(received.get());
  }

  /**
   * Tests rejecting an event that indicates event messages were lost.
   */
  public void testSessionRejectMissingEvent() throws Throwable {
    ClientSessionListener listener = createListener();

    AtomicBoolean received = new AtomicBoolean();
    listener.onEvent("foo", value -> {
      assertEquals(value, "Hello world!");
      received.set(true);
    });

    PublishResponse response;
    response = handler.handle(PublishRequest.builder()
      .withSession(1)
      .withEventIndex(10)
      .withPreviousIndex(2)
      .withEvents(new Event<String>("foo", "Hello world!"))
      .build()).get();

    assertEquals(response.status(), Response.Status.ERROR);
    assertEquals(response.index(), 1);
    assertEquals(state.getEventIndex(), 1);
    assertFalse(received.get());
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
