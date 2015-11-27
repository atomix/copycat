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

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.ServiceLoaderTypeResolver;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.LocalServerRegistry;
import io.atomix.catalyst.transport.LocalTransport;
import io.atomix.catalyst.util.concurrent.SingleThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.error.RaftError;
import io.atomix.copycat.client.response.AbstractResponse;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.TestStateMachine;
import io.atomix.copycat.server.Testing.ThrowableRunnable;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.copycat.server.storage.TestEntry;
import io.atomix.copycat.server.storage.entry.Entry;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Abstract state test.
 */
@Test
public abstract class AbstractStateTest<T extends AbstractState> extends ConcurrentTestCase {
  protected T state;
  protected Serializer serializer;
  protected Storage storage;
  protected Log log;
  protected TestStateMachine stateMachine;
  protected ThreadContext serverCtx;
  protected LocalTransport transport;
  protected ServerState serverState;
  protected List<Member> members;

  /**
   * Sets up a server state.
   */
  @BeforeMethod
  void beforeMethod() throws Throwable {
    serializer = new Serializer();
    serializer.resolve(new ServiceLoaderTypeResolver());

    storage = new Storage(StorageLevel.MEMORY);
    storage.serializer().resolve(new ServiceLoaderTypeResolver());

    log = storage.open("test");
    stateMachine = new TestStateMachine();
    members = createMembers(3);
    transport = new LocalTransport(new LocalServerRegistry());

    serverCtx = new SingleThreadContext("test-server", serializer);
    serverState = new ServerState(members.get(0), members.stream().map(Member::serverAddress).collect(Collectors.toList()), log, stateMachine, new ConnectionManager(transport.client()), serverCtx);
  }

  /**
   * Clears test logs.
   */
  @AfterMethod
  void afterMethod() throws Throwable {
    log.close();
    serverCtx.close();
  }

  /**
   * Appends the given number of entries in the given term on the server. Must be run on server's ThreadContext.
   */
  protected void append(int entries, long term) throws Throwable {
    for (int i = 0; i < entries; i++) {
      try (TestEntry entry = serverState.getLog().create(TestEntry.class)) {
        entry.setTerm(term).setTombstone(false);
        serverState.getLog().append(entry);
      }
    }
  }

  /**
   * Gets the entry at the given index.
   */
  protected <T extends Entry> T get(long index) throws Throwable {
    return serverState.getLog().get(index);
  }

  /**
   * Runs the runnable on the server context. Failures within the {@code runnable} are rethrown on the main test thread.
   */
  protected void runOnServer(ThrowableRunnable runnable) throws Throwable {
    serverCtx.execute(() -> {
      try {
        runnable.run();
        resume();
      } catch (Throwable t) {
        rethrow(t);
      }
    });
    await();
  }

  /**
   * Creates and returns the given number of entries in the given term.
   */
  protected List<TestEntry> entries(int entries, long term) {
    List<TestEntry> result = new ArrayList<>();
    for (int i = 0; i < entries; i++) {
      try (TestEntry entry = serverState.getLog().create(TestEntry.class)) {
        result.add(entry.setTerm(term).setTombstone(false));
      }
    }
    return result;
  }

  protected void assertNoLeaderError(AbstractResponse<?> response) {
    threadAssertEquals(response.status(), Response.Status.ERROR);
    threadAssertEquals(response.error(), RaftError.Type.NO_LEADER_ERROR);
  }

  protected void assertIllegalMemberStateError(AbstractResponse<?> response) {
    threadAssertEquals(response.status(), Response.Status.ERROR);
    threadAssertEquals(response.error(), RaftError.Type.ILLEGAL_MEMBER_STATE_ERROR);
  }

  /**
   * Creates a collection of member addresses.
   */
  private List<Member> createMembers(int nodes) {
    List<Member> members = new ArrayList<>();
    for (int i = 0; i < nodes; i++) {
      members.add(new Member(RaftMemberType.ACTIVE, new Address("localhost", 5000 + i), new Address("localhost", 6000 + i)));
    }
    return members;
  }

}
