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
import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.atomix.copycat.protocol.ProtocolListener;
import io.atomix.copycat.protocol.request.PublishRequest;
import io.atomix.copycat.protocol.response.PublishResponse;
import io.atomix.copycat.protocol.websocket.request.WebSocketPublishRequest;
import io.atomix.copycat.protocol.websocket.response.WebSocketPublishResponse;
import io.atomix.copycat.protocol.websocket.response.WebSocketResponse;
import io.atomix.copycat.session.Event;
import io.atomix.copycat.session.Session;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

/**
 * Client session listener test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ClientSessionListenerTest {
  private ClientSessionState state;
  private ProtocolListener<PublishRequest, PublishResponse.Builder, PublishResponse> listener;

  /**
   * Creates a client session listener.
   */
  @SuppressWarnings("unchecked")
  private ClientSessionListener createListener() throws Throwable {
    ArgumentCaptor<ProtocolListener> captor = ArgumentCaptor.forClass(ProtocolListener.class);
    ProtocolClientConnection connection = mock(ProtocolClientConnection.class);

    state = new ClientSessionState(UUID.randomUUID().toString());
    Executor executor = new MockExecutor();
    ThreadContext context = mock(ThreadContext.class);
    when(context.executor()).thenReturn(executor);

    state.setSessionId(1).setState(Session.State.OPEN);

    ClientSessionListener listener = new ClientSessionListener(connection, state, new ClientSequencer(state), context);
    verify(connection).onPublish(captor.capture());
    this.listener = captor.getValue();
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
    response = this.listener.onRequest(new WebSocketPublishRequest.Builder(1)
      .withSession(1)
      .withEventIndex(10)
      .withPreviousIndex(1)
      .withEvents(new Event<String>("foo", "Hello world!"))
      .build(), new WebSocketPublishResponse.Builder(1)).get();

    assertEquals(response.status(), WebSocketResponse.Status.OK);
    assertEquals(response.index(), 10);
    assertEquals(state.getEventIndex(), 10);
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
    response = this.listener.onRequest(new WebSocketPublishRequest.Builder(1)
      .withSession(1)
      .withEventIndex(10)
      .withPreviousIndex(1)
      .withEvents(new Event<String>("foo", "Hello world!"))
      .build(), new WebSocketPublishResponse.Builder(1)).get();

    assertEquals(response.status(), WebSocketResponse.Status.OK);
    assertEquals(response.index(), 10);
    assertEquals(state.getEventIndex(), 10);
    assertTrue(received.get());

    received.set(false);
    response = this.listener.onRequest(new WebSocketPublishRequest.Builder(2)
      .withSession(1)
      .withEventIndex(10)
      .withPreviousIndex(1)
      .withEvents(new Event<String>("foo", "Hello world!"))
      .build(), new WebSocketPublishResponse.Builder(2)).get();

    assertEquals(response.status(), WebSocketResponse.Status.OK);
    assertEquals(response.index(), 10);
    assertEquals(state.getEventIndex(), 10);
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
    response = this.listener.onRequest(new WebSocketPublishRequest.Builder(1)
      .withSession(1)
      .withEventIndex(10)
      .withPreviousIndex(2)
      .withEvents(new Event<String>("foo", "Hello world!"))
      .build(), new WebSocketPublishResponse.Builder(2)).get();

    assertEquals(response.status(), WebSocketResponse.Status.ERROR);
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
