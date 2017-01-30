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

import io.atomix.copycat.util.concurrent.Listener;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.*;

/**
 * Client session state test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ClientSessionStateTest {

  /**
   * Tests session state defaults.
   */
  public void testSessionStateDefaults() {
    String clientId = UUID.randomUUID().toString();
    ClientSessionState state = new ClientSessionState(clientId);
    assertEquals(state.getClientId(), clientId);
    assertEquals(state.getSessionId(), 0);
    assertEquals(state.getState(), ClientSession.State.CLOSED);
    assertEquals(state.getCommandRequest(), 0);
    assertEquals(state.getCommandResponse(), 0);
    assertEquals(state.getResponseIndex(), 0);
    assertEquals(state.getEventIndex(), 0);
  }

  /**
   * Tests updating client session state.
   */
  public void testSessionState() {
    ClientSessionState state = new ClientSessionState(UUID.randomUUID().toString());
    assertEquals(state.setSessionId(1).getSessionId(), 1);
    assertEquals(state.getResponseIndex(), 1);
    assertEquals(state.getEventIndex(), 1);
    assertEquals(state.setState(ClientSession.State.OPEN).getState(), ClientSession.State.OPEN);
    assertEquals(state.setCommandRequest(2).getCommandRequest(), 2);
    assertEquals(state.nextCommandRequest(), 3);
    assertEquals(state.getCommandRequest(), 3);
    assertEquals(state.setCommandResponse(3).getCommandResponse(), 3);
    assertEquals(state.setResponseIndex(4).getResponseIndex(), 4);
    assertEquals(state.setResponseIndex(3).getResponseIndex(), 4);
    assertEquals(state.setEventIndex(5).getEventIndex(), 5);
  }

  /**
   * Tests session state change callbacks.
   */
  public void testSessionStateChange() {
    ClientSessionState state = new ClientSessionState(UUID.randomUUID().toString());
    AtomicBoolean changed = new AtomicBoolean();
    AtomicReference<ClientSession.State> change = new AtomicReference<>();
    Listener<ClientSession.State> listener = state.onStateChange(s -> {
      changed.set(true);
      change.set(s);
    });

    assertEquals(state.getState(), ClientSession.State.CLOSED);
    state.setState(ClientSession.State.CLOSED);
    assertFalse(changed.get());

    state.setState(ClientSession.State.OPEN);
    assertTrue(changed.get());
    assertEquals(change.get(), ClientSession.State.OPEN);

    changed.set(false);
    listener.close();

    state.setState(ClientSession.State.EXPIRED);
    assertFalse(changed.get());
  }

}
