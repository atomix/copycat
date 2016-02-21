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

import io.atomix.catalyst.util.Listener;
import io.atomix.copycat.session.Session;
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
    UUID clientId = UUID.randomUUID();
    ClientSessionState state = new ClientSessionState(clientId);
    assertEquals(state.getClientId(), clientId);
    assertEquals(state.getSessionId(), 0);
    assertEquals(state.getState(), Session.State.CLOSED);
    assertEquals(state.getCommandRequest(), 0);
    assertEquals(state.getCommandResponse(), 0);
    assertEquals(state.getResponseIndex(), 0);
    assertEquals(state.getEventIndex(), 0);
    assertEquals(state.getCompleteIndex(), 0);
  }

  /**
   * Tests updating client session state.
   */
  public void testSessionState() {
    ClientSessionState state = new ClientSessionState(UUID.randomUUID());
    assertEquals(state.setSessionId(1).getSessionId(), 1);
    assertEquals(state.getResponseIndex(), 1);
    assertEquals(state.getEventIndex(), 1);
    assertEquals(state.getCompleteIndex(), 1);
    assertEquals(state.setState(Session.State.OPEN).getState(), Session.State.OPEN);
    assertEquals(state.setCommandRequest(2).getCommandRequest(), 2);
    assertEquals(state.nextCommandRequest(), 3);
    assertEquals(state.getCommandRequest(), 3);
    assertEquals(state.setCommandResponse(3).getCommandResponse(), 3);
    assertEquals(state.setResponseIndex(4).getResponseIndex(), 4);
    assertEquals(state.setResponseIndex(3).getResponseIndex(), 4);
    assertEquals(state.setEventIndex(5).getEventIndex(), 5);
    assertEquals(state.setCompleteIndex(6).getCompleteIndex(), 6);
    assertEquals(state.setCompleteIndex(5).getCompleteIndex(), 6);
  }

  /**
   * Tests session state change callbacks.
   */
  public void testSessionStateChange() {
    ClientSessionState state = new ClientSessionState(UUID.randomUUID());
    AtomicBoolean changed = new AtomicBoolean();
    AtomicReference<Session.State> change = new AtomicReference<>();
    Listener<Session.State> listener = state.onStateChange(s -> {
      changed.set(true);
      change.set(s);
    });

    assertEquals(state.getState(), Session.State.CLOSED);
    state.setState(Session.State.CLOSED);
    assertFalse(changed.get());

    state.setState(Session.State.OPEN);
    assertTrue(changed.get());
    assertEquals(change.get(), Session.State.OPEN);

    changed.set(false);
    listener.close();

    state.setState(Session.State.EXPIRED);
    assertFalse(changed.get());
  }

}
