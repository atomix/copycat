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

import io.atomix.copycat.protocol.ProtocolClientConnection;
import io.atomix.copycat.protocol.ProtocolListener;
import io.atomix.copycat.protocol.request.PublishRequest;
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.protocol.response.PublishResponse;
import io.atomix.copycat.util.concurrent.ThreadContext;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.UUID;
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
  private ProtocolListener<PublishRequest, PublishResponse> listener;

  /**
   * Creates a client session listener.
   */
  @SuppressWarnings("unchecked")
  private ClientSessionListener createListener() throws Throwable {
    ArgumentCaptor<ProtocolListener> captor = ArgumentCaptor.forClass(ProtocolListener.class);
    ProtocolClientConnection connection = mock(ProtocolClientConnection.class);

    state = new ClientSessionState(UUID.randomUUID().toString());
    ThreadContext context = mock(ThreadContext.class);
    doAnswer((a) -> {
      ((Runnable) a.getArguments()[0]).run();
      return null;
    }).when(context).execute(any(Runnable.class));

    state.setSessionId(1).setState(ClientSession.State.OPEN);

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
    listener.onEvent(value -> {
      assertEquals(value, "Hello world!".getBytes());
      received.set(true);
    });

    PublishResponse response;
    response = this.listener.onRequest(PublishRequest.builder()
      .withSession(1)
      .withEventIndex(10)
      .withPreviousIndex(1)
      .withEvents(Collections.singletonList("Hello world!".getBytes()))
      .build()).get();

    assertEquals(response.status(), ProtocolResponse.Status.OK);
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
    listener.onEvent(value -> {
      assertEquals(value, "Hello world!".getBytes());
      received.set(true);
    });

    PublishResponse response;
    response = this.listener.onRequest(PublishRequest.builder()
      .withSession(1)
      .withEventIndex(10)
      .withPreviousIndex(1)
      .withEvents(Collections.singletonList("Hello world!".getBytes()))
      .build()).get();

    assertEquals(response.status(), ProtocolResponse.Status.OK);
    assertEquals(response.index(), 10);
    assertEquals(state.getEventIndex(), 10);
    assertTrue(received.get());

    received.set(false);
    response = this.listener.onRequest(PublishRequest.builder()
      .withSession(1)
      .withEventIndex(10)
      .withPreviousIndex(1)
      .withEvents(Collections.singletonList("Hello world!".getBytes()))
      .build()).get();

    assertEquals(response.status(), ProtocolResponse.Status.OK);
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
    listener.onEvent(value -> {
      assertEquals(value, "Hello world!".getBytes());
      received.set(true);
    });

    PublishResponse response;
    response = this.listener.onRequest(PublishRequest.builder()
      .withSession(1)
      .withEventIndex(10)
      .withPreviousIndex(2)
      .withEvents(Collections.singletonList("Hello world!".getBytes()))
      .build()).get();

    assertEquals(response.status(), ProtocolResponse.Status.ERROR);
    assertEquals(response.index(), 1);
    assertEquals(state.getEventIndex(), 1);
    assertFalse(received.get());
  }
}
