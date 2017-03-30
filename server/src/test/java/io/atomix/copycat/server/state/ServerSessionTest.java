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
package io.atomix.copycat.server.state;

import io.atomix.copycat.server.storage.Log;
import org.testng.annotations.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.*;

/**
 * Server session test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ServerSessionTest {

  /**
   * Tests session command sequencing.
   */
  public void testInitializeSession() throws Throwable {
    ServerStateMachineContext context = mock(ServerStateMachineContext.class);
    ServerSessionContext session = new ServerSessionContext(10, UUID.randomUUID().toString(), mock(Log.class), context, 1000);
    assertEquals(session.id(), 10);
    assertEquals(session.getLastCompleted(), 9);
    assertEquals(session.getLastApplied(), 9);
  }

  /**
   * Tests sequencing an index query.
   */
  public void testSequenceIndexQuery() throws Throwable {
    ServerStateMachineContext context = mock(ServerStateMachineContext.class);
    ServerSessionContext session = new ServerSessionContext(10, UUID.randomUUID().toString(), mock(Log.class), context, 1000);
    AtomicBoolean complete = new AtomicBoolean();
    session.registerIndexQuery(10, () -> complete.set(true));
    assertFalse(complete.get());
    session.setLastApplied(9);
    assertFalse(complete.get());
    session.setLastApplied(10);
    assertTrue(complete.get());
  }

  /**
   * Tests sequencing a sequence query.
   */
  public void testSequenceSequenceQuery() throws Throwable {
    ServerStateMachineContext context = mock(ServerStateMachineContext.class);
    ServerSessionContext session = new ServerSessionContext(10, UUID.randomUUID().toString(), mock(Log.class), context, 1000);
    AtomicBoolean complete = new AtomicBoolean();
    session.registerSequenceQuery(10, () -> complete.set(true));
    assertFalse(complete.get());
    session.setCommandSequence(9);
    assertFalse(complete.get());
    session.setCommandSequence(10);
    assertTrue(complete.get());
  }

  /**
   * Tests caching a response.
   */
  public void testCacheResponse() throws Throwable {
    ServerStateMachineContext context = mock(ServerStateMachineContext.class);
    ServerSessionContext session = new ServerSessionContext(10, UUID.randomUUID().toString(), mock(Log.class), context, 1000);
    session.registerResult(2, new ServerStateMachine.Result(2, 2, "Hello world!"));
    assertEquals(session.getResult(2).result, "Hello world!");
    session.clearResults(3);
    assertNull(session.getResult(2));
  }

}
