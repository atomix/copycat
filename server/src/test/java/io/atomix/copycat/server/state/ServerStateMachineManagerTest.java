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

import io.atomix.catalyst.concurrent.CatalystThreadFactory;
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
import io.atomix.copycat.server.storage.util.StorageSerialization;
import io.atomix.copycat.server.util.ServerSerialization;
import io.atomix.copycat.util.ProtocolSerialization;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Server state machine test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ServerStateMachineManagerTest extends ConcurrentTestCase {
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

    new SingleThreadContext("test", serializer.clone()).execute(() -> {
      state = new ServerContext(
        "test",
        member.type(),
        member.serverAddress(),
        member.clientAddress(),
        storage,
        serializer,
        new StateMachineRegistry().register("test", TestStateMachine::new),
        new ConnectionManager(new LocalTransport(registry).client()),
        Executors.newScheduledThreadPool(2, new CatalystThreadFactory("test-%d")),
        callerContext);
      resume();
    });
    await(1000);
    timestamp = System.currentTimeMillis();
    sequence = new AtomicLong();
  }

  // TODO: Add tests!

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
