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

import io.atomix.copycat.client.request.Request;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.request.VoteRequest;
import io.atomix.copycat.server.response.VoteResponse;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.ServiceLoaderTypeResolver;
import io.atomix.catalyst.transport.*;
import io.atomix.catalyst.util.concurrent.SingleThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Server context test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ServerStateTest extends ConcurrentTestCase {
  private static final File directory = new File("test-logs");
  private LocalServerRegistry registry;
  private Transport transport;
  private List<Address> members;
  private Storage storage;
  private Log log;
  private TestStateMachine stateMachine;
  private Serializer serializer;
  private ThreadContext context;
  private ThreadContext test;
  private UUID id;
  private Client client;
  private Server server;
  private Connection connection;
  private ServerState state;

  /**
   * Sets up a server state.
   */
  @BeforeMethod
  public void createState() throws Throwable {
    deleteDirectory(directory);

    serializer = new Serializer();
    serializer.resolve(new ServiceLoaderTypeResolver());

    registry = new LocalServerRegistry();
    transport = new LocalTransport(registry, serializer);
    members = createMembers(3);
    storage = new Storage(StorageLevel.MEMORY);
    log = storage.open("test");
    stateMachine = new TestStateMachine();
    context = new SingleThreadContext("test-server", serializer);
    test = new SingleThreadContext("test-context", serializer.clone());

    storage.serializer().resolve(new ServiceLoaderTypeResolver());

    id = UUID.randomUUID();

    server = transport.server(id);
    client = transport.client(id);

    state = new ServerState(members.get(0), members, log, stateMachine, new ConnectionManager(client), context);

    context.execute(() -> {
      server.listen(members.get(0), state::connect).whenComplete((result, error) -> resume());
    });
    await();

    test.execute(() -> {
      client.connect(members.get(0)).whenComplete((result, error) -> {
        threadAssertNull(error);
        this.connection = result;
        resume();
      });
    });
    await();
  }

  /**
   * Sets up server state.
   */
  private void configure(Consumer<ServerState> callback) throws Throwable {
    context.execute(() -> {
      callback.accept(state);
      resume();
    });
    await();
  }

  /**
   * Transitions the server state.
   */
  private void transition(CopycatServer.State state) throws Throwable {
    context.execute(() -> {
      this.state.transition(state).join();
      resume();
    });
    await();
  }

  /**
   * Tests a server response.
   */
  private <T extends Request<T>, U extends Response<U>> void test(T request, Consumer<U> callback) throws Throwable {
    test.execute(() -> {
      connection.<T, U>send(request).whenComplete((response, error) -> {
        threadAssertNull(error);
        callback.accept(response);
        resume();
      });
    });
    await();
  }

  /**
   * Tests a follower handling a vote request.
   */
  public void testFollowerVoteRequest() throws Throwable {
    configure(state -> {
      state.setTerm(1).setLeader(0);
    });

    transition(CopycatServer.State.FOLLOWER);

    VoteRequest request = VoteRequest.builder()
      .withTerm(2)
      .withCandidate(members.get(1).hashCode())
      .withLogIndex(0)
      .withLogTerm(0)
      .build();

    this.<VoteRequest, VoteResponse>test(request, response -> {
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.voted());
    });
  }

  /**
   * Test state machine.
   */
  private static class TestStateMachine extends StateMachine {
    @Override
    protected void configure(StateMachineExecutor executor) {

    }
  }

  /**
   * Creates a collection of member addresses.
   */
  private List<Address> createMembers(int nodes) {
    List<Address> members = new ArrayList<>();
    for (int i = 0; i < nodes; i++) {
      members.add(new Address("localhost", 5000 + i));
    }
    return members;
  }

  /**
   * Clears test logs.
   */
  @AfterMethod
  public void destroyState() throws Throwable {
    context.execute(() -> server.close().whenComplete((result, error) -> resume()));
    await();
    test.execute(() -> client.close().whenComplete((result, error) -> resume()));
    await();
    log.close();
    context.close();
    test.close();
    deleteDirectory(directory);
  }

  /**
   * Deletes a directory recursively.
   */
  private void deleteDirectory(File directory) throws IOException {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteDirectory(file);
          } else {
            Files.delete(file.toPath());
          }
        }
      }
      Files.delete(directory.toPath());
    }
  }

}
