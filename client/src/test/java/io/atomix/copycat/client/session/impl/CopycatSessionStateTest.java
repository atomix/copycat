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

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.copycat.client.session.impl.CopycatSessionState;
import io.atomix.copycat.session.Session;
import org.testng.annotations.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Client session state test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class CopycatSessionStateTest {

  /**
   * Tests session state defaults.
   */
  public void testSessionStateDefaults() {
    String sessionName = UUID.randomUUID().toString();
    CopycatSessionState state = new CopycatSessionState(1, sessionName, "test");
    assertEquals(state.getSessionId(), 0);
    assertEquals(state.getSessionName(), sessionName);
    assertEquals(state.getSessionType(), "test");
    assertEquals(state.getCommandRequest(), 0);
    assertEquals(state.getCommandResponse(), 0);
    assertEquals(state.getResponseIndex(), 0);
    assertEquals(state.getEventIndex(), 0);
  }

  /**
   * Tests updating client session state.
   */
  public void testSessionState() {
    CopycatSessionState state = new CopycatSessionState(1, UUID.randomUUID().toString(), "test");
    assertEquals(state.getSessionId(), 1);
    assertEquals(state.getResponseIndex(), 1);
    assertEquals(state.getEventIndex(), 1);
    assertEquals(state.setCommandRequest(2).getCommandRequest(), 2);
    assertEquals(state.nextCommandRequest(), 3);
    assertEquals(state.getCommandRequest(), 3);
    assertEquals(state.setCommandResponse(3).getCommandResponse(), 3);
    assertEquals(state.setResponseIndex(4).getResponseIndex(), 4);
    assertEquals(state.setResponseIndex(3).getResponseIndex(), 4);
    assertEquals(state.setEventIndex(5).getEventIndex(), 5);
  }

}
