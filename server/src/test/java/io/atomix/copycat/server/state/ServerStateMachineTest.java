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
 * limitations under the License.
 */
package io.atomix.copycat.server.state;

import io.atomix.catalyst.concurrent.SingleThreadContext;
import io.atomix.catalyst.concurrent.ThreadContext;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.Transport;
import io.atomix.catalyst.transport.local.LocalServerRegistry;
import io.atomix.catalyst.transport.local.LocalTransport;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.protocol.ClientRequestTypeResolver;
import io.atomix.copycat.protocol.ClientResponseTypeResolver;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.copycat.server.storage.entry.*;
import io.atomix.copycat.server.storage.util.StorageSerialization;
import io.atomix.copycat.server.util.ServerSerialization;
import io.atomix.copycat.session.Session;
import io.atomix.copycat.util.ProtocolSerialization;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.testng.Assert.*;

/**
 * Server state machine test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ServerStateMachineTest extends ConcurrentTestCase {
  private ThreadContext callerContext;
  private ThreadContext stateContext;
  private Transport transport;
  private ServerContext state;
  private long timestamp;
  private AtomicLong sequence;

  @BeforeMethod
  public void createStateMachine() throws Throwable {
    Serializer serializer = new Serializer().resolve(
      new ClientRequestTypeResolver(),
      new ClientResponseTypeResolver(),
      new ProtocolSerialization(),
      new ServerSerialization(),
      new StorageSerialization()
    ).disableWhitelist();

    callerContext = new SingleThreadContext("caller", serializer.clone());
    stateContext = new SingleThreadContext("state", serializer.clone());
    LocalServerRegistry registry = new LocalServerRegistry();
    transport = new LocalTransport(registry);
    Storage storage = new Storage(StorageLevel.MEMORY);
    ServerMember member = new ServerMember(Member.Type.ACTIVE, new Address("localhost", 5000), new Address("localhost", 6000), Instant.now());

    new SingleThreadContext("test", serializer.clone()).executor().execute(() -> {
      state = new ServerContext("test", member.type(), member.serverAddress(), member.clientAddress(), storage, serializer, TestStateMachine::new, new ConnectionManager(new LocalTransport(registry).client()), callerContext);
      resume();
    });
    await(1000);
    timestamp = System.currentTimeMillis();
    sequence = new AtomicLong();
  }

  /**
   * Tests registering a session.
   */
  public void testSessionRegisterKeepAlive() throws Throwable {
    callerContext.execute(() -> {

      long index;
      try (RegisterEntry entry = state.getLog().create(RegisterEntry.class)) {
        entry.setTerm(1)
          .setTimestamp(timestamp)
          .setTimeout(500)
          .setClient(UUID.randomUUID().toString());
        index = state.getLog().append(entry);
      }

      state.getStateMachine().apply(index).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    ServerSessionContext session = state.getStateMachine().executor().context().sessions().getSession(1);
    assertNotNull(session);
    assertEquals(session.id(), 1);
    assertEquals(session.getTimestamp(), timestamp);

    callerContext.execute(() -> {

      long index;
      try (KeepAliveEntry entry = state.getLog().create(KeepAliveEntry.class)) {
        entry.setTerm(1)
          .setSession(1)
          .setTimestamp(timestamp + 1000)
          .setCommandSequence(0)
          .setEventIndex(0);
        index = state.getLog().append(entry);
      }

      state.getStateMachine().apply(index).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    assertEquals(session.getTimestamp(), timestamp + 1000);
  }

  /**
   * Tests resetting session timeouts when a new leader is elected.
   */
  public void testSessionLeaderReset() throws Throwable {
    callerContext.execute(() -> {

      long index;
      try (RegisterEntry entry = state.getLog().create(RegisterEntry.class)) {
        entry.setTerm(1)
          .setTimestamp(timestamp)
          .setTimeout(500)
          .setClient(UUID.randomUUID().toString());
        index = state.getLog().append(entry);
      }

      state.getStateMachine().apply(index).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    ServerSessionContext session = state.getStateMachine().executor().context().sessions().getSession(1);
    assertNotNull(session);
    assertEquals(session.id(), 1);
    assertEquals(session.getTimestamp(), timestamp);

    callerContext.execute(() -> {

      long index;
      try (InitializeEntry entry = state.getLog().create(InitializeEntry.class)) {
        entry.setTerm(1)
          .setTimestamp(timestamp + 100);
        index = state.getLog().append(entry);
      }

      state.getStateMachine().apply(index).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    assertEquals(session.getTimestamp(), timestamp + 100);
  }

  /**
   * Tests expiring a session.
   */
  public void testSessionSuspect() throws Throwable {
    callerContext.execute(() -> {

      long index;
      try (RegisterEntry entry = state.getLog().create(RegisterEntry.class)) {
        entry.setTerm(1)
          .setTimestamp(timestamp)
          .setTimeout(500)
          .setClient(UUID.randomUUID().toString());
        index = state.getLog().append(entry);
      }

      state.getStateMachine().apply(index).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    ServerSessionContext session = state.getStateMachine().executor().context().sessions().getSession(1);
    assertNotNull(session);
    assertEquals(session.id(), 1);
    assertEquals(session.getTimestamp(), timestamp);

    callerContext.execute(() -> {

      long index;
      try (KeepAliveEntry entry = state.getLog().create(KeepAliveEntry.class)) {
        entry.setTerm(1)
          .setSession(2)
          .setTimestamp(timestamp + 1000)
          .setCommandSequence(0)
          .setEventIndex(0);
        index = state.getLog().append(entry);
      }

      state.getStateMachine().apply(index).whenComplete((result, error) -> {
        threadAssertNotNull(error);
        resume();
      });
    });

    await();

    assertTrue(session.state() == Session.State.UNSTABLE);
  }

  /**
   * Tests executing an asynchronous callback in the state machine.
   */
  public void testExecute() throws Throwable {
    callerContext.execute(() -> {

      long index;
      try (RegisterEntry entry = state.getLog().create(RegisterEntry.class)) {
        entry.setTerm(1)
          .setTimestamp(timestamp)
          .setTimeout(500)
          .setClient(UUID.randomUUID().toString());
        index = state.getLog().append(entry);
      }

      state.getStateMachine().apply(index).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    ServerSessionContext session = state.getStateMachine().executor().context().sessions().getSession(1);
    assertNotNull(session);
    assertEquals(session.id(), 1);
    assertEquals(session.getTimestamp(), timestamp);
    assertEquals(session.getCommandSequence(), 0);

    callerContext.execute(() -> {

      long index;
      try (CommandEntry entry = state.getLog().create(CommandEntry.class)) {
        entry.setTerm(1)
          .setSession(1)
          .setSequence(1)
          .setTimestamp(timestamp + 100)
          .setCommand(new TestExecute());
        index = state.getLog().append(entry);
      }

      state.getStateMachine().<ServerStateMachine.Result>apply(index).whenComplete((result, error) -> {
        threadAssertNull(result.result);
        threadAssertNull(error);
        resume();
      });

    });

    await(1000, 2);
  }

  /**
   * Tests command sequencing.
   */
  public void testCommandSequence() throws Throwable {
    callerContext.execute(() -> {

      long index;
      try (RegisterEntry entry = state.getLog().create(RegisterEntry.class)) {
        entry.setTerm(1)
          .setTimestamp(timestamp)
          .setTimeout(500)
          .setClient(UUID.randomUUID().toString());
        index = state.getLog().append(entry);
      }

      state.getStateMachine().apply(index).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });
    });

    await();

    ServerSessionContext session = state.getStateMachine().executor().context().sessions().getSession(1);
    assertNotNull(session);
    assertEquals(session.id(), 1);
    assertEquals(session.getTimestamp(), timestamp);
    assertEquals(session.getCommandSequence(), 0);

    callerContext.execute(() -> {

      long index;
      try (CommandEntry entry = state.getLog().create(CommandEntry.class)) {
        entry.setTerm(1)
          .setSession(1)
          .setSequence(1)
          .setTimestamp(timestamp + 100)
          .setCommand(new TestCommand());
        index = state.getLog().append(entry);
      }

      state.getStateMachine().<ServerStateMachine.Result>apply(index).whenComplete((result, error) -> {
        threadAssertEquals(result.result, 1L);
        resume();
      });

    });

    await();

    assertEquals(session.getCommandSequence(), 1);
    assertEquals(session.getTimestamp(), timestamp + 100);

    callerContext.execute(() -> {

      long index;
      try (CommandEntry entry = state.getLog().create(CommandEntry.class)) {
        entry.setTerm(1)
          .setSession(1)
          .setSequence(2)
          .setTimestamp(timestamp + 200)
          .setCommand(new TestCommand());
        index = state.getLog().append(entry);
      }

      state.getStateMachine().<ServerStateMachine.Result>apply(index).whenComplete((result, error) -> {
        threadAssertEquals(result.result, 2L);
        resume();
      });

    });

    callerContext.execute(() -> {

      long index;
      try (CommandEntry entry = state.getLog().create(CommandEntry.class)) {
        entry.setTerm(1)
          .setSession(1)
          .setSequence(3)
          .setTimestamp(timestamp + 300)
          .setCommand(new TestCommand());
        index = state.getLog().append(entry);
      }

      state.getStateMachine().<ServerStateMachine.Result>apply(index).whenComplete((result, error) -> {
        threadAssertEquals(result.result, 3L);
        resume();
      });

    });

    await(1000, 2);

    assertEquals(session.getCommandSequence(), 3);
    assertEquals(session.getTimestamp(), timestamp + 300);
  }

  /**
   * Tests serializing queries.
   */
  public void testQuerySerialize() throws Throwable {
    callerContext.execute(() -> {

      long index;
      try (RegisterEntry entry = state.getLog().create(RegisterEntry.class)) {
        entry.setTerm(1)
          .setTimestamp(timestamp)
          .setTimeout(500)
          .setClient(UUID.randomUUID().toString());
        index = state.getLog().append(entry);
      }

      state.getStateMachine().apply(index).whenComplete((result, error) -> {
        threadAssertNull(error);
        resume();
      });

      threadAssertEquals(state.getStateMachine().getLastApplied(), index);
    });

    await();

    ServerSessionContext session = state.getStateMachine().executor().context().sessions().getSession(1);
    assertNotNull(session);
    assertEquals(session.id(), 1);
    assertEquals(session.getTimestamp(), timestamp);
    assertEquals(session.getCommandSequence(), 0);

    callerContext.execute(() -> {

      QueryEntry entry = state.getLog().create(QueryEntry.class);
        entry.setIndex(1)
          .setTerm(1)
          .setSession(1)
          .setTimestamp(timestamp + 200)
          .setSequence(0)
          .setQuery(new TestQuery());

      state.getStateMachine().<ServerStateMachine.Result>apply(entry).whenComplete((result, error) -> {
        threadAssertEquals(result.result, 1L);
        resume();
      });
    });

    callerContext.execute(() -> {

      long index;
      try (CommandEntry entry = state.getLog().create(CommandEntry.class)) {
        entry.setTerm(1)
          .setSession(1)
          .setSequence(1)
          .setTimestamp(timestamp + 100)
          .setCommand(new TestCommand());
        index = state.getLog().append(entry);
      }

      state.getStateMachine().<ServerStateMachine.Result>apply(index).whenComplete((result, error) -> {
        threadAssertEquals(result.result, 2L);
        resume();
      });

      threadAssertEquals(state.getStateMachine().getLastApplied(), index);
    });

    await(1000, 2);

    assertEquals(session.getCommandSequence(), 1);
    assertEquals(session.getTimestamp(), timestamp + 100);
  }

  @AfterMethod
  public void closeStateMachine() {
    state.close();
    callerContext.close();
  }

  /**
   * Test state machine.
   */
  private class TestStateMachine extends StateMachine {
    @Override
    public void configure(StateMachineExecutor executor) {
      executor.register(TestCommand.class, this::testCommand);
      executor.register(TestQuery.class, this::testQuery);
      executor.register(EventCommand.class, this::eventCommand);
      executor.register(TestExecute.class, this::testExecute);
    }

    private long testCommand(Commit<TestCommand> commit) {
      return sequence.incrementAndGet();
    }

    private void eventCommand(Commit<EventCommand> commit) {
      commit.session().publish("hello", "world!");
    }

    private long testQuery(Commit<TestQuery> commit) {
      return sequence.incrementAndGet();
    }

    private void testExecute(Commit<TestExecute> commit) {
      executor.execute((Runnable) () -> {
        threadAssertEquals(context.index(), 2L);
        resume();
      });
    }
  }

  /**
   * Test command.
   */
  private static class TestCommand implements Command<Long> {
  }

  /**
   * Event command.
   */
  private static class EventCommand implements Command<Void> {
  }

  /**
   * Test query.
   */
  private static class TestQuery implements Query<Long> {
  }

  /**
   * Test execute.
   */
  private static class TestExecute implements Command<Void> {
  }

}
