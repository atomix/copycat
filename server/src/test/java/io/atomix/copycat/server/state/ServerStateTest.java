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
import io.atomix.catalyst.transport.*;
import io.atomix.catalyst.util.concurrent.SingleThreadContext;
import io.atomix.catalyst.util.concurrent.ThreadContext;
import io.atomix.copycat.client.Command;
import io.atomix.copycat.client.request.CommandRequest;
import io.atomix.copycat.client.request.Request;
import io.atomix.copycat.client.response.CommandResponse;
import io.atomix.copycat.client.response.Response;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.StateMachineExecutor;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.request.PollRequest;
import io.atomix.copycat.server.request.VoteRequest;
import io.atomix.copycat.server.response.AppendResponse;
import io.atomix.copycat.server.response.PollResponse;
import io.atomix.copycat.server.response.VoteResponse;
import io.atomix.copycat.server.storage.Log;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.copycat.server.storage.TestEntry;
import io.atomix.copycat.server.storage.entry.CommandEntry;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Server context test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ServerStateTest extends ConcurrentTestCase {
  private LocalServerRegistry registry;
  private Transport transport;
  private List<Address> members;
  private Storage storage;
  private Log log;
  private TestStateMachine stateMachine;
  private Serializer serializer;
  private ThreadContext context;
  private ThreadContext test;
  private Client client;
  private Server server;
  private Connection connection;
  private ServerState state;

  /**
   * Sets up a server state.
   */
  @BeforeMethod
  public void createState() throws Throwable {
    serializer = new Serializer();
    serializer.resolve(new ServiceLoaderTypeResolver());

    storage = new Storage(StorageLevel.MEMORY);
    storage.serializer().resolve(new ServiceLoaderTypeResolver());

    registry = new LocalServerRegistry();
    transport = new LocalTransport(registry, serializer);
    members = createMembers(3);
    log = storage.open("test");
    stateMachine = new TestStateMachine();
    context = new SingleThreadContext("test-server", serializer);
    test = new SingleThreadContext("test-context", serializer.clone());

    server = transport.server();
    client = transport.client();

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
   * Appends the given number of entries in the given term.
   */
  private void append(int entries, long term) throws Throwable {
    context.execute(() -> {
      for (int i = 0; i < entries; i++) {
        try (TestEntry entry = state.getLog().create(TestEntry.class)) {
          entry.setTerm(term)
            .setAddress(i);
          state.getLog().append(entry);
        }
      }
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
   * Tests a follower handling a vote request with a higher term.
   */
  public void testFollowerIncrementsTermOnPollRequest() throws Throwable {
    configure(state -> {
      state.setTerm(1).setLeader(0);
    });

    transition(CopycatServer.State.FOLLOWER);

    PollRequest request = PollRequest.builder()
      .withTerm(2)
      .withCandidate(members.get(1).hashCode())
      .withLogIndex(0)
      .withLogTerm(0)
      .build();

    this.<PollRequest, PollResponse>test(request, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.accepted());
    });
  }

  /**
   * Tests that a follower rejects a poll request with a lower term.
   */
  public void testFollowerRejectsPollRequestWithLowerTerm() throws Throwable {
    configure(state -> {
      state.setTerm(2).setLeader(0);
    });

    transition(CopycatServer.State.FOLLOWER);

    PollRequest request = PollRequest.builder()
      .withTerm(1)
      .withCandidate(members.get(1).hashCode())
      .withLogIndex(0)
      .withLogTerm(0)
      .build();

    this.<PollRequest, PollResponse>test(request, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertFalse(response.accepted());
    });
  }

  /**
   * Tests that a follower will accept a poll for many candidates.
   */
  public void testFollowerAcceptsPollForMultipleCandidatesPerTerm() throws Throwable {
    configure(state -> {
      state.setTerm(2).setLeader(0);
    });

    transition(CopycatServer.State.FOLLOWER);

    PollRequest request1 = PollRequest.builder()
      .withTerm(2)
      .withCandidate(members.get(1).hashCode())
      .withLogIndex(0)
      .withLogTerm(0)
      .build();

    this.<PollRequest, PollResponse>test(request1, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.accepted());
    });

    PollRequest request2 = PollRequest.builder()
      .withTerm(2)
      .withCandidate(members.get(2).hashCode())
      .withLogIndex(0)
      .withLogTerm(0)
      .build();

    this.<PollRequest, PollResponse>test(request2, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.accepted());
    });
  }

  /**
   * Tests that a follower rejects a poll when the candidate's log is not up to date.
   */
  public void testFollowerRejectsPollWhenLogNotUpToDate() throws Throwable {
    configure(state -> {
      state.setTerm(2).setLeader(0);
    });

    transition(CopycatServer.State.FOLLOWER);

    append(10, 1);

    PollRequest request = PollRequest.builder()
      .withTerm(2)
      .withCandidate(members.get(1).hashCode())
      .withLogIndex(5)
      .withLogTerm(1)
      .build();

    this.<PollRequest, PollResponse>test(request, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertFalse(response.accepted());
    });
  }

  /**
   * Tests that a follower accepts a poll when the candidate's log is up to date.
   */
  public void testFollowerAcceptsPollWhenLogUpToDate() throws Throwable {
    configure(state -> {
      state.setTerm(2).setLeader(0);
    });

    transition(CopycatServer.State.FOLLOWER);

    append(10, 1);

    PollRequest request = PollRequest.builder()
      .withTerm(2)
      .withCandidate(members.get(1).hashCode())
      .withLogIndex(11)
      .withLogTerm(2)
      .build();

    this.<PollRequest, PollResponse>test(request, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.accepted());
    });
  }

  /**
   * Tests a follower handling a vote request.
   */
  public void testFollowerIncrementsTermOnVoteRequest() throws Throwable {
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
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.voted());
    });
  }

  /**
   * Tests that a follower rejects a vote request
   * @throws Throwable
   */
  public void testFollowerRejectsVoteRequestWithLowerTerm() throws Throwable {
    configure(state -> {
      state.setTerm(2).setLeader(0);
    });

    transition(CopycatServer.State.FOLLOWER);

    VoteRequest request = VoteRequest.builder()
      .withTerm(1)
      .withCandidate(members.get(1).hashCode())
      .withLogIndex(0)
      .withLogTerm(0)
      .build();

    this.<VoteRequest, VoteResponse>test(request, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertFalse(response.voted());
    });
  }

  /**
   * Tests that a follower will only vote for one candidate.
   */
  public void testFollowerVotesForOneCandidatePerTerm() throws Throwable {
    configure(state -> {
      state.setTerm(2).setLeader(0);
    });

    transition(CopycatServer.State.FOLLOWER);

    VoteRequest request1 = VoteRequest.builder()
      .withTerm(2)
      .withCandidate(members.get(1).hashCode())
      .withLogIndex(0)
      .withLogTerm(0)
      .build();

    this.<VoteRequest, VoteResponse>test(request1, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(state.getLastVotedFor(), members.get(1).hashCode());
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.voted());
    });

    VoteRequest request2 = VoteRequest.builder()
      .withTerm(2)
      .withCandidate(members.get(2).hashCode())
      .withLogIndex(0)
      .withLogTerm(0)
      .build();

    this.<VoteRequest, VoteResponse>test(request2, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(state.getLastVotedFor(), members.get(1).hashCode());
      threadAssertEquals(response.term(), 2L);
      threadAssertFalse(response.voted());
    });
  }

  /**
   * Tests that a follower overrides a vote from a previous term when the term increases.
   */
  public void testFollowerOverridesVoteForNewTerm() throws Throwable {
    configure(state -> {
      state.setTerm(2).setLeader(0);
    });

    transition(CopycatServer.State.FOLLOWER);

    VoteRequest request1 = VoteRequest.builder()
      .withTerm(2)
      .withCandidate(members.get(1).hashCode())
      .withLogIndex(0)
      .withLogTerm(0)
      .build();

    this.<VoteRequest, VoteResponse>test(request1, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(state.getLastVotedFor(), members.get(1).hashCode());
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.voted());
    });

    VoteRequest request2 = VoteRequest.builder()
      .withTerm(3)
      .withCandidate(members.get(2).hashCode())
      .withLogIndex(0)
      .withLogTerm(0)
      .build();

    this.<VoteRequest, VoteResponse>test(request2, response -> {
      threadAssertEquals(state.getTerm(), 3L);
      threadAssertEquals(state.getLastVotedFor(), members.get(2).hashCode());
      threadAssertEquals(response.term(), 3L);
      threadAssertTrue(response.voted());
    });
  }

  /**
   * Tests that a follower rejects a vote when the candidate's log is not up to date.
   */
  public void testFollowerRejectsVoteWhenLogNotUpToDate() throws Throwable {
    configure(state -> {
      state.setTerm(2).setLeader(0);
    });

    transition(CopycatServer.State.FOLLOWER);

    append(10, 1);

    VoteRequest request = VoteRequest.builder()
      .withTerm(2)
      .withCandidate(members.get(1).hashCode())
      .withLogIndex(5)
      .withLogTerm(1)
      .build();

    this.<VoteRequest, VoteResponse>test(request, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertFalse(response.voted());
    });
  }

  /**
   * Tests that a follower accepts a vote when the candidate's log is up to date.
   */
  public void testFollowerAcceptsVoteWhenLogUpToDate() throws Throwable {
    configure(state -> {
      state.setTerm(2).setLeader(0);
    });

    transition(CopycatServer.State.FOLLOWER);

    append(10, 1);

    VoteRequest request = VoteRequest.builder()
      .withTerm(2)
      .withCandidate(members.get(1).hashCode())
      .withLogIndex(11)
      .withLogTerm(2)
      .build();

    this.<VoteRequest, VoteResponse>test(request, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.voted());
    });
  }

  /**
   * Tests that a follower updates a leader on append request with new term.
   */
  public void testFollowerUpdatesLeaderAndTermOnAppend() throws Throwable {
    configure(state -> {
      state.setTerm(2).setLeader(0);
    });

    transition(CopycatServer.State.FOLLOWER);

    VoteRequest request1 = VoteRequest.builder()
      .withTerm(2)
      .withCandidate(members.get(1).hashCode())
      .withLogIndex(0)
      .withLogTerm(0)
      .build();

    this.<VoteRequest, VoteResponse>test(request1, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(state.getLastVotedFor(), members.get(1).hashCode());
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.voted());
    });

    AppendRequest request2 = AppendRequest.builder()
      .withTerm(3)
      .withLeader(members.get(2).hashCode())
      .withLogIndex(0)
      .withLogTerm(0)
      .withCommitIndex(0)
      .withGlobalIndex(0)
      .build();

    this.<AppendRequest, AppendResponse>test(request2, response -> {
      threadAssertEquals(state.getTerm(), 3L);
      threadAssertEquals(state.getLeader(), members.get(2));
      threadAssertEquals(state.getLastVotedFor(), 0);
      threadAssertEquals(response.term(), 3L);
      threadAssertTrue(response.succeeded());
    });
  }

  /**
   * Tests that a leader steps down when it receives a higher term.
   */
  public void testLeaderStepsDownAndVotesOnHigherTerm() throws Throwable {
    configure(state -> {
      state.setTerm(1).setLeader(0);
    });

    transition(CopycatServer.State.LEADER);

    VoteRequest request = VoteRequest.builder()
      .withTerm(2)
      .withCandidate(members.get(1).hashCode())
      .withLogIndex(11)
      .withLogTerm(2)
      .build();

    state.onStateChange(state -> {
      if (state == CopycatServer.State.FOLLOWER) {
        resume();
      }
    });

    this.<VoteRequest, VoteResponse>test(request, response -> {
      threadAssertEquals(state.getTerm(), 2L);
      threadAssertEquals(state.getLastVotedFor(), members.get(1).hashCode());
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.voted());
    });

    await();
  }

  /**
   * Tests that the leader sequences commands to the log.
   */
  public void testLeaderSequencesCommands() throws Throwable {
    configure(state -> {
      state.setTerm(1).setLeader(members.get(0).hashCode()).getStateMachine().executor().context().sessions().registerSession(new ServerSession(1, state.getStateMachine().executor().context(), 1000));
    });

    transition(CopycatServer.State.LEADER);

    CommandRequest request1 = CommandRequest.builder()
      .withSession(1)
      .withSequence(2)
      .withCommand(new TestCommand("bar"))
      .build();

    CommandRequest request2 = CommandRequest.builder()
      .withSession(1)
      .withSequence(3)
      .withCommand(new TestCommand("baz"))
      .build();

    CommandRequest request3 = CommandRequest.builder()
      .withSession(1)
      .withSequence(1)
      .withCommand(new TestCommand("foo"))
      .build();

    AtomicInteger counter = new AtomicInteger();

    test.execute(() -> {
      connection.<CommandRequest, CommandResponse>send(request1);
      connection.<CommandRequest, CommandResponse>send(request2);
      connection.<CommandRequest, CommandResponse>send(request3);
      resume();
    });

    await();

    Thread.sleep(1000);

    state.getThreadContext().execute(() -> {

      try (CommandEntry entry = state.getLog().get(2)) {
        threadAssertEquals(((TestCommand) entry.getOperation()).value, "foo");
      }

      try (CommandEntry entry = state.getLog().get(3)) {
        threadAssertEquals(((TestCommand) entry.getOperation()).value, "bar");
      }

      try (CommandEntry entry = state.getLog().get(4)) {
        threadAssertEquals(((TestCommand) entry.getOperation()).value, "baz");
      }

      resume();
    });

    await();
  }

  /**
   * Test state machine.
   */
  private static class TestStateMachine extends StateMachine {
    @Override
    protected void configure(StateMachineExecutor executor) {
      executor.register(TestCommand.class, this::command);
    }

    private String command(Commit<TestCommand> commit) {
      return commit.operation().value;
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
  }

  /**
   * Test command.
   */
  public static class TestCommand implements Command<String> {
    private String value;

    public TestCommand(String value) {
      this.value = value;
    }
  }

}
