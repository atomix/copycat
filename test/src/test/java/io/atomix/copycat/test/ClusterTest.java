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
package io.atomix.copycat.test;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.LocalServerRegistry;
import io.atomix.catalyst.transport.LocalTransport;
import io.atomix.catalyst.util.Listener;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.DefaultCopycatClient;
import io.atomix.copycat.client.RetryStrategies;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.session.ServerSession;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Copycat cluster test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ClusterTest extends ConcurrentTestCase {
  protected volatile int port;
  protected volatile LocalServerRegistry registry;
  protected volatile List<Member> members;
  protected volatile List<CopycatClient> clients = new ArrayList<>();
  protected volatile List<CopycatServer> servers = new ArrayList<>();

  /**
   * Tests starting several members individually.
   */
  public void testSingleMemberStart() throws Throwable {
    CopycatServer server = createServers(1).get(0);
    server.start().thenRun(this::resume);
    await(5000);
    CopycatServer joiner1 = createServer(members, nextMember(Member.Type.ACTIVE));
    joiner1.start().thenRun(this::resume);
    await(5000);
    CopycatServer joiner2 = createServer(members, nextMember(Member.Type.ACTIVE));
    joiner2.start().thenRun(this::resume);
    await(5000);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  public void testActiveJoinLate() throws Throwable {
    testServerJoinLate(Member.Type.ACTIVE, CopycatServer.State.FOLLOWER);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  public void testPassiveJoinLate() throws Throwable {
    testServerJoinLate(Member.Type.PASSIVE, CopycatServer.State.PASSIVE);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  public void testReserveJoinLate() throws Throwable {
    testServerJoinLate(Member.Type.RESERVE, CopycatServer.State.RESERVE);
  }

  /**
   * Tests joining a server after many entries have been committed.
   */
  private void testServerJoinLate(Member.Type type, CopycatServer.State state) throws Throwable {
    createServers(3);
    CopycatClient client = createClient();
    submit(client, 0, 1000);
    await(30000);
    CopycatServer joiner = createServer(members, nextMember(type));
    joiner.onStateChange(s -> {
      if (s == state)
        resume();
    });
    joiner.start().thenRun(this::resume);
    await(30000, 2);
  }

  /**
   * Submits a bunch of commands recursively.
   */
  private void submit(CopycatClient client, int count, int total) {
    if (count < total) {
      client.submit(new TestCommand("Hello world!", Command.ConsistencyLevel.LINEARIZABLE)).whenComplete((result, error) -> {
        threadAssertNull(error);
        submit(client, count + 1, total);
      });
    } else {
      resume();
    }
  }

  /**
   * Tests joining a server to an existing cluster.
   */
  public void testCrashRecover() throws Throwable {
    List<CopycatServer> servers = createServers(3);
    CopycatClient client = createClient();
    submit(client, 0, 1000);
    await(30000);
    servers.get(0).kill().join();
    CopycatServer server = createServer(members, members.get(0));
    server.start().thenRun(this::resume);
    await(30000);
    submit(client, 0, 1000);
    await(30000);
  }

  /**
   * Tests leaving a sever from a cluster.
   */
  public void testServerLeave() throws Throwable {
    List<CopycatServer> servers = createServers(3);
    CopycatServer server = servers.get(0);
    server.stop().thenRun(this::resume);
    await(30000);
  }

  /**
   * Tests leaving the leader from a cluster.
   */
  public void testLeaderLeave() throws Throwable {
    List<CopycatServer> servers = createServers(3);
    CopycatServer server = servers.stream().filter(s -> s.state() == CopycatServer.State.LEADER).findFirst().get();
    server.stop().thenRun(this::resume);
    await(30000);
  }

  /**
   * Tests keeping a client session alive.
   */
  public void testClientKeepAlive() throws Throwable {
    createServers(3);
    CopycatClient client = createClient();
    Thread.sleep(Duration.ofSeconds(10).toMillis());
    threadAssertTrue(client.state() == CopycatClient.State.CONNECTED);
  }

  /**
   * Tests an active member joining the cluster.
   */
  public void testActiveJoin() throws Throwable {
    testServerJoin(Member.Type.ACTIVE);
  }

  /**
   * Tests a passive member joining the cluster.
   */
  public void testPassiveJoin() throws Throwable {
    testServerJoin(Member.Type.PASSIVE);
  }

  /**
   * Tests a reserve member joining the cluster.
   */
  public void testReserveJoin() throws Throwable {
    testServerJoin(Member.Type.RESERVE);
  }

  /**
   * Tests a server joining the cluster.
   */
  private void testServerJoin(Member.Type type) throws Throwable {
    createServers(3);
    CopycatServer server = createServer(members, nextMember(type));
    server.start().thenRun(this::resume);
    await(10000);
  }

  /**
   * Tests joining and leaving the cluster, resizing the quorum.
   */
  public void testResize() throws Throwable {
    CopycatServer server = createServers(1).get(0);
    CopycatServer joiner = createServer(members, nextMember(Member.Type.ACTIVE));
    joiner.start().thenRun(this::resume);
    await(10000);
    server.stop().thenRun(this::resume);
    await(10000);
    joiner.stop().thenRun(this::resume);
  }

  /**
   * Tests an availability change of an active member.
   */
  public void testActiveAvailabilityChange() throws Throwable {
    testAvailabilityChange(Member.Type.ACTIVE);
  }

  /**
   * Tests an availability change of a passive member.
   */
  public void testPassiveAvailabilityChange() throws Throwable {
    testAvailabilityChange(Member.Type.PASSIVE);
  }

  /**
   * Tests an availability change of a reserve member.
   */
  public void testReserveAvailabilityChange() throws Throwable {
    testAvailabilityChange(Member.Type.RESERVE);
  }

  /**
   * Tests a member availability change.
   */
  private void testAvailabilityChange(Member.Type type) throws Throwable {
    List<CopycatServer> servers = createServers(3);

    CopycatServer server = servers.get(0);
    server.cluster().onJoin(m -> {
      m.onStatusChange(s -> {
        threadAssertEquals(s, Member.Status.UNAVAILABLE);
        resume();
      });
    });

    Member member = nextMember(type);
    CopycatServer joiner = createServer(members, member);
    joiner.start().thenRun(this::resume);
    await(10000);

    joiner.kill().thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests detecting an availability change of a reserve member on a passive member.
   */
  public void testPassiveReserveAvailabilityChange() throws Throwable {
    createServers(3);

    CopycatServer passive = createServer(members, nextMember(Member.Type.PASSIVE));
    passive.start().thenRun(this::resume);

    await(10000);

    Member reserveMember = nextMember(Member.Type.RESERVE);
    passive.cluster().onJoin(member -> {
      threadAssertEquals(member.address(), reserveMember.address());
      member.onStatusChange(s -> {
        threadAssertEquals(s, Member.Status.UNAVAILABLE);
        resume();
      });
    });

    CopycatServer reserve = createServer(members, reserveMember);
    reserve.start().thenRun(this::resume);

    await(10000);

    reserve.kill().thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests detecting an availability change of a passive member on a reserve member.
   */
  public void testReservePassiveAvailabilityChange() throws Throwable {
    createServers(3);

    CopycatServer passive = createServer(members, nextMember(Member.Type.PASSIVE));
    passive.start().thenRun(this::resume);

    CopycatServer reserve = createServer(members, nextMember(Member.Type.RESERVE));
    reserve.start().thenRun(this::resume);

    await(10000, 2);

    reserve.cluster().member(passive.cluster().member().address()).onStatusChange(s -> {
      threadAssertEquals(s, Member.Status.UNAVAILABLE);
      resume();
    });

    passive.kill().thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests an active member join event.
   */
  public void testActiveJoinEvent() throws Throwable {
    testJoinEvent(Member.Type.ACTIVE);
  }

  /**
   * Tests a passive member join event.
   */
  public void testPassiveJoinEvent() throws Throwable {
    testJoinEvent(Member.Type.PASSIVE);
  }

  /**
   * Tests a reserve member join event.
   */
  public void testReserveJoinEvent() throws Throwable {
    testJoinEvent(Member.Type.RESERVE);
  }

  /**
   * Tests a member join event.
   */
  private void testJoinEvent(Member.Type type) throws Throwable {
    List<CopycatServer> servers = createServers(3);

    Member member = nextMember(type);

    CopycatServer server = servers.get(0);
    server.cluster().onJoin(m -> {
      threadAssertEquals(m.address(), member.address());
      threadAssertEquals(m.type(), type);
      resume();
    });

    CopycatServer joiner = createServer(members, member);
    joiner.start().thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests demoting the leader.
   */
  public void testDemoteLeader() throws Throwable {
    List<CopycatServer> servers = createServers(3);

    CopycatServer leader = servers.stream()
      .filter(s -> s.cluster().member().equals(s.cluster().leader()))
      .findFirst()
      .get();

    CopycatServer follower = servers.stream()
      .filter(s -> !s.cluster().member().equals(s.cluster().leader()))
      .findFirst()
      .get();

    follower.cluster().member(leader.cluster().member().address()).onTypeChange(t -> {
      threadAssertEquals(t, Member.Type.PASSIVE);
      resume();
    });
    leader.cluster().member().demote(Member.Type.PASSIVE).thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests submitting a command.
   */
  public void testOneNodeSubmitCommandWithSequentialConsistency() throws Throwable {
    testSubmitCommand(1, Command.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a command.
   */
  public void testOneNodeSubmitCommandWithLinearizableConsistency() throws Throwable {
    testSubmitCommand(1, Command.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a command.
   */
  public void testTwoNodeSubmitCommandWithSequentialConsistency() throws Throwable {
    testSubmitCommand(2, Command.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a command.
   */
  public void testTwoNodeSubmitCommandWithLinearizableConsistency() throws Throwable {
    testSubmitCommand(2, Command.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a command.
   */
  public void testThreeNodeSubmitCommandWithSequentialConsistency() throws Throwable {
    testSubmitCommand(3, Command.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a command.
   */
  public void testThreeNodeSubmitCommandWithLinearizableConsistency() throws Throwable {
    testSubmitCommand(3, Command.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a command.
   */
  public void testFourNodeSubmitCommandWithSequentialConsistency() throws Throwable {
    testSubmitCommand(4, Command.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a command.
   */
  public void testFourNodeSubmitCommandWithLinearizableConsistency() throws Throwable {
    testSubmitCommand(4, Command.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a command.
   */
  public void testFiveNodeSubmitCommandWithSequentialConsistency() throws Throwable {
    testSubmitCommand(5, Command.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a command.
   */
  public void testFiveNodeSubmitCommandWithLinearizableConsistency() throws Throwable {
    testSubmitCommand(5, Command.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a command with a configured consistency level.
   */
  private void testSubmitCommand(int nodes, Command.ConsistencyLevel consistency) throws Throwable {
    createServers(nodes);

    CopycatClient client = createClient();
    client.submit(new TestCommand("Hello world!", consistency)).thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await(30000);
  }

  /**
   * Tests submitting a command.
   */
  public void testTwoOfThreeNodeSubmitCommandWithSequentialConsistency() throws Throwable {
    testSubmitCommand(2, 3, Command.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a command.
   */
  public void testTwoOfThreeNodeSubmitCommandWithLinearizableConsistency() throws Throwable {
    testSubmitCommand(2, 3, Command.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a command.
   */
  public void testThreeOfFourNodeSubmitCommandWithSequentialConsistency() throws Throwable {
    testSubmitCommand(3, 4, Command.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a command.
   */
  public void testThreeOfFourNodeSubmitCommandWithLinearizableConsistency() throws Throwable {
    testSubmitCommand(3, 4, Command.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a command.
   */
  public void testThreeOfFiveNodeSubmitCommandWithSequentialConsistency() throws Throwable {
    testSubmitCommand(3, 5, Command.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a command.
   */
  public void testThreeOfFiveNodeSubmitCommandWithLinearizableConsistency() throws Throwable {
    testSubmitCommand(3, 5, Command.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a command to a partial cluster.
   */
  private void testSubmitCommand(int live, int total, Command.ConsistencyLevel consistency) throws Throwable {
    createServers(live, total);

    CopycatClient client = createClient();
    client.submit(new TestCommand("Hello world!", consistency)).thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await(30000);
  }

  /**
   * Tests submitting a query.
   */
  public void testOneNodeSubmitQueryWithCausalConsistency() throws Throwable {
    testSubmitQuery(1, Query.ConsistencyLevel.CAUSAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testOneNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(1, Query.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testOneNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(1, Query.ConsistencyLevel.BOUNDED_LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  public void testOneNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(1, Query.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  public void testTwoNodeSubmitQueryWithCausalConsistency() throws Throwable {
    testSubmitQuery(2, Query.ConsistencyLevel.CAUSAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testTwoNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(2, Query.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testTwoNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(2, Query.ConsistencyLevel.BOUNDED_LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  public void testTwoNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(2, Query.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  public void testThreeNodeSubmitQueryWithCausalConsistency() throws Throwable {
    testSubmitQuery(3, Query.ConsistencyLevel.CAUSAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testThreeNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(3, Query.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testThreeNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(3, Query.ConsistencyLevel.BOUNDED_LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  public void testThreeNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(3, Query.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  public void testFourNodeSubmitQueryWithCausalConsistency() throws Throwable {
    testSubmitQuery(4, Query.ConsistencyLevel.CAUSAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testFourNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(4, Query.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testFourNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(4, Query.ConsistencyLevel.BOUNDED_LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  public void testFourNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(4, Query.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  public void testFiveNodeSubmitQueryWithCausalConsistency() throws Throwable {
    testSubmitQuery(5, Query.ConsistencyLevel.CAUSAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testFiveNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(5, Query.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testFiveNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(5, Query.ConsistencyLevel.BOUNDED_LINEARIZABLE);
  }

  /**
   * Tests submitting a query.
   */
  public void testFiveNodeSubmitQueryWithLinearizableConsistency() throws Throwable {
    testSubmitQuery(5, Query.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests submitting a query with a configured consistency level.
   */
  private void testSubmitQuery(int nodes, Query.ConsistencyLevel consistency) throws Throwable {
    createServers(nodes);

    CopycatClient client = createClient();
    client.submit(new TestQuery("Hello world!", consistency)).thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await(30000);
  }

  /**
   * Tests submitting a sequential event.
   */
  public void testOneNodeSequentialEvent() throws Throwable {
    testSequentialEvent(1);
  }

  /**
   * Tests submitting a sequential event.
   */
  public void testTwoNodeSequentialEvent() throws Throwable {
    testSequentialEvent(2);
  }

  /**
   * Tests submitting a sequential event.
   */
  public void testThreeNodeSequentialEvent() throws Throwable {
    testSequentialEvent(3);
  }

  /**
   * Tests submitting a sequential event.
   */
  public void testFourNodeSequentialEvent() throws Throwable {
    testSequentialEvent(4);
  }

  /**
   * Tests submitting a sequential event.
   */
  public void testFiveNodeSequentialEvent() throws Throwable {
    testSequentialEvent(5);
  }

  /**
   * Tests submitting a sequential event.
   */
  private void testSequentialEvent(int nodes) throws Throwable {
    createServers(nodes);

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertEquals(message, "Hello world!");
      resume();
    });

    client.submit(new TestEvent("Hello world!", true, Command.ConsistencyLevel.SEQUENTIAL)).thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await(30000, 2);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testOneNodeSequentialEvents() throws Throwable {
    testSequentialEvents(1);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testTwoNodeSequentialEvents() throws Throwable {
    testSequentialEvents(2);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testThreeNodeSequentialEvents() throws Throwable {
    testSequentialEvents(3);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testFourNodeSequentialEvents() throws Throwable {
    testSequentialEvents(4);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testFiveNodeSequentialEvents() throws Throwable {
    testSequentialEvents(5);
  }

  /**
   * Tests submitting sequential events to all sessions.
   */
  private void testSequentialEvents(int nodes) throws Throwable {
    createServers(nodes);

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertEquals(message, "Hello world!");
      resume();
    });
    createClient().onEvent("test", message -> {
      threadAssertEquals(message, "Hello world!");
      resume();
    });
    createClient().onEvent("test", message -> {
      threadAssertEquals(message, "Hello world!");
      resume();
    });

    client.submit(new TestEvent("Hello world!", false, Command.ConsistencyLevel.SEQUENTIAL)).thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    await(30000, 4);
  }

  /**
   * Tests submitting a linearizable event.
   */
  public void testOneNodeLinearizableEvent() throws Throwable {
    testLinearizableEvent(1);
  }

  /**
   * Tests submitting a linearizable event.
   */
  public void testTwoNodeLinearizableEvent() throws Throwable {
    testLinearizableEvent(2);
  }

  /**
   * Tests submitting a linearizable event.
   */
  public void testThreeNodeLinearizableEvent() throws Throwable {
    testLinearizableEvent(3);
  }

  /**
   * Tests submitting a linearizable event.
   */
  public void testFourNodeLinearizableEvent() throws Throwable {
    testLinearizableEvent(4);
  }

  /**
   * Tests submitting a linearizable event.
   */
  public void testFiveNodeLinearizableEvent() throws Throwable {
    testLinearizableEvent(5);
  }

  /**
   * Tests submitting an event command.
   */
  private void testLinearizableEvent(int nodes) throws Throwable {
    createServers(nodes);

    AtomicInteger counter = new AtomicInteger();

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertEquals(message, "Hello world!");
      counter.incrementAndGet();
      resume();
    });

    client.submit(new TestEvent("Hello world!", true, Command.ConsistencyLevel.LINEARIZABLE)).thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      threadAssertEquals(counter.get(), 1);
      resume();
    });

    await(30000, 2);
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testOneNodeLinearizableEvents() throws Throwable {
    testLinearizableEvents(1);
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testTwoNodeLinearizableEvents() throws Throwable {
    testLinearizableEvents(2);
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testThreeNodeLinearizableEvents() throws Throwable {
    testLinearizableEvents(3);
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testFourNodeLinearizableEvents() throws Throwable {
    testLinearizableEvents(4);
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testFiveNodeLinearizableEvents() throws Throwable {
    testLinearizableEvents(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testLinearizableEvents(int nodes) throws Throwable {
    createServers(nodes);

    AtomicInteger counter = new AtomicInteger();

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertEquals(message, "Hello world!");
      counter.incrementAndGet();
      resume();
    });
    createClient().onEvent("test", message -> {
      threadAssertEquals(message, "Hello world!");
      counter.incrementAndGet();
      resume();
    });
    createClient().onEvent("test", message -> {
      threadAssertEquals(message, "Hello world!");
      counter.incrementAndGet();
      resume();
    });

    client.submit(new TestEvent("Hello world!", false, Command.ConsistencyLevel.LINEARIZABLE)).thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      threadAssertEquals(counter.get(), 3);
      resume();
    });

    await(30000, 4);
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testFiveNodeCommandBeforeEvent() throws Throwable {
    testCommandBeforeEvent(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testCommandBeforeEvent(int nodes) throws Throwable {
    createServers(nodes);

    AtomicInteger counter = new AtomicInteger();

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertEquals(message, "Hello world!");
      counter.incrementAndGet();
      resume();
    });

    client.submit(new TestCommand("Hello world!", Command.ConsistencyLevel.LINEARIZABLE)).thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      resume();
    });

    client.submit(new TestEvent("Hello world!", true, Command.ConsistencyLevel.LINEARIZABLE)).thenAccept(result -> {
      threadAssertEquals(result, "Hello world!");
      threadAssertEquals(counter.get(), 1);
      resume();
    });

    await(30000, 3);
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testFiveNodeManyEvents() throws Throwable {
    testManyEvents(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testManyEvents(int nodes) throws Throwable {
    createServers(nodes);

    AtomicReference<String> value = new AtomicReference<>();

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertEquals(message, value.get());
      resume();
    });

    for (int i = 0 ; i < 10; i++) {
      String event = UUID.randomUUID().toString();
      value.set(event);
      client.submit(new TestEvent(event, true, Command.ConsistencyLevel.LINEARIZABLE)).thenAccept(result -> {
        threadAssertEquals(result, event);
        resume();
      });

      await(30000, 2);
    }
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testThreeNodesManyEventsAfterLeaderShutdown() throws Throwable {
    testManyEventsAfterLeaderShutdown(3);
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testFiveNodesManyEventsAfterLeaderShutdown() throws Throwable {
    testManyEventsAfterLeaderShutdown(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testManyEventsAfterLeaderShutdown(int nodes) throws Throwable {
    List<CopycatServer> servers = createServers(nodes);

    AtomicReference<String> value = new AtomicReference<>();

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertEquals(message, value.get());
      resume();
    });

    for (int i = 0 ; i < 10; i++) {
      String event = UUID.randomUUID().toString();
      value.set(event);
      client.submit(new TestEvent(event, true, Command.ConsistencyLevel.LINEARIZABLE)).thenAccept(result -> {
        threadAssertEquals(result, event);
        resume();
      });

      await(30000, 2);
    }

    CopycatServer leader = servers.stream().filter(s -> s.state() == CopycatServer.State.LEADER).findFirst().get();
    leader.stop().join();

    for (int i = 0 ; i < 10; i++) {
      String event = UUID.randomUUID().toString();
      value.set(event);
      client.submit(new TestEvent(event, true, Command.ConsistencyLevel.LINEARIZABLE)).thenAccept(result -> {
        threadAssertEquals(result, event);
        resume();
      });

      await(30000, 2);
    }
  }

  /**
   * Tests submitting sequential events.
   */
  public void testThreeNodesSequentialEventsAfterFollowerKill() throws Throwable {
    testSequentialEventsAfterFollowerKill(3);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testFiveNodesSequentialEventsAfterFollowerKill() throws Throwable {
    testSequentialEventsAfterFollowerKill(5);
  }

  /**
   * Tests submitting a sequential event that publishes to all sessions.
   */
  private void testSequentialEventsAfterFollowerKill(int nodes) throws Throwable {
    List<CopycatServer> servers = createServers(nodes);

    AtomicReference<String> value = new AtomicReference<>();

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertEquals(message, value.get());
      resume();
    });

    for (int i = 0 ; i < 10; i++) {
      String event = UUID.randomUUID().toString();
      value.set(event);
      client.submit(new TestEvent(event, true, Command.ConsistencyLevel.SEQUENTIAL)).thenAccept(result -> {
        threadAssertEquals(result, event);
        resume();
      });

      await(30000, 2);
    }

    String singleEvent = UUID.randomUUID().toString();
    value.set(singleEvent);
    client.submit(new TestEvent(singleEvent, true, Command.ConsistencyLevel.SEQUENTIAL)).thenAccept(result -> {
      threadAssertEquals(result, singleEvent);
      resume();
    });

    CopycatServer follower = servers.stream().filter(s -> s.state() == CopycatServer.State.FOLLOWER).findFirst().get();
    follower.kill().join();

    await(30000, 2);

    for (int i = 0 ; i < 10; i++) {
      String event = UUID.randomUUID().toString();
      value.set(event);
      client.submit(new TestEvent(event, true, Command.ConsistencyLevel.SEQUENTIAL)).thenAccept(result -> {
        threadAssertEquals(result, event);
        resume();
      });

      await(30000, 2);
    }
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testFiveNodesLinearizableEventsAfterLeaderKill() throws Throwable {
    testLinearizableEventsAfterLeaderKill(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testLinearizableEventsAfterLeaderKill(int nodes) throws Throwable {
    List<CopycatServer> servers = createServers(nodes);

    AtomicReference<String> value = new AtomicReference<>();

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertEquals(message, value.get());
      resume();
    });

    for (int i = 0 ; i < 10; i++) {
      String event = UUID.randomUUID().toString();
      value.set(event);
      client.submit(new TestEvent(event, true, Command.ConsistencyLevel.LINEARIZABLE)).thenAccept(result -> {
        threadAssertEquals(result, event);
        resume();
      });

      await(30000, 2);
    }

    String singleEvent = UUID.randomUUID().toString();
    value.set(singleEvent);
    client.submit(new TestEvent(singleEvent, true, Command.ConsistencyLevel.LINEARIZABLE)).thenAccept(result -> {
      threadAssertEquals(result, singleEvent);
      resume();
    });

    CopycatServer leader = servers.stream().filter(s -> s.state() == CopycatServer.State.LEADER).findFirst().get();
    leader.kill().join();

    await(30000, 2);

    for (int i = 0 ; i < 10; i++) {
      String event = UUID.randomUUID().toString();
      value.set(event);
      client.submit(new TestEvent(event, true, Command.ConsistencyLevel.LINEARIZABLE)).thenAccept(result -> {
        threadAssertEquals(result, event);
        resume();
      });

      await(30000, 2);
    }
  }

  /**
   * Tests submitting linearizable events.
   */
  public void testFiveNodeManySessionsManyEvents() throws Throwable {
    testManySessionsManyEvents(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testManySessionsManyEvents(int nodes) throws Throwable {
    createServers(nodes);

    AtomicReference<String> value = new AtomicReference<>();

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertEquals(message, value.get());
      resume();
    });

    createClient().onEvent("test", message -> {
      threadAssertEquals(message, value.get());
      resume();
    });

    createClient().onEvent("test", message -> {
      threadAssertEquals(message, value.get());
      resume();
    });

    for (int i = 0; i < 10; i++) {
      String event = UUID.randomUUID().toString();
      value.set(event);
      client.submit(new TestEvent(event, false, Command.ConsistencyLevel.LINEARIZABLE)).thenAccept(result -> {
        threadAssertEquals(result, event);
        resume();
      });

      await(10000, 4);
    }
  }

  /**
   * Tests session expiring events.
   */
  public void testOneNodeExpireEvent() throws Throwable {
    testSessionExpire(1);
  }

  /**
   * Tests session expiring events.
   */
  public void testThreeNodeExpireEvent() throws Throwable {
    testSessionExpire(3);
  }

  /**
   * Tests session expiring events.
   */
  public void testFiveNodeExpireEvent() throws Throwable {
    testSessionExpire(5);
  }

  /**
   * Tests a session expiring.
   */
  private void testSessionExpire(int nodes) throws Throwable {
    createServers(nodes);

    CopycatClient client1 = createClient();
    CopycatClient client2 = createClient();
    client1.onEvent("expired", this::resume);
    client1.submit(new TestExpire()).thenRun(this::resume);
    ((DefaultCopycatClient) client2).kill().thenRun(this::resume);
    await(Duration.ofSeconds(10).toMillis(), 3);
  }

  /**
   * Tests session close events.
   */
  public void testOneNodeCloseEvent() throws Throwable {
    testSessionClose(1);
  }

  /**
   * Tests session close events.
   */
  public void testThreeNodeCloseEvent() throws Throwable {
    testSessionClose(3);
  }

  /**
   * Tests session close events.
   */
  public void testFiveNodeCloseEvent() throws Throwable {
    testSessionClose(5);
  }

  /**
   * Tests a session closing.
   */
  private void testSessionClose(int nodes) throws Throwable {
    createServers(nodes);

    CopycatClient client1 = createClient();
    CopycatClient client2 = createClient();
    client1.submit(new TestClose()).thenRun(this::resume);
    await(Duration.ofSeconds(10).toMillis(), 1);
    client1.onEvent("closed", this::resume);
    client2.close().thenRun(this::resume);
    await(Duration.ofSeconds(10).toMillis(), 2);
  }

  /**
   * Returns the next server address.
   *
   * @return The next server address.
   */
  private Member nextMember() {
    return nextMember(Member.Type.INACTIVE);
  }

  /**
   * Returns the next server address.
   *
   * @param type The startup member type.
   * @return The next server address.
   */
  private Member nextMember(Member.Type type) {
    return new TestMember(type, new Address("localhost", ++port), new Address("localhost", port + 1000));
  }

  /**
   * Creates a set of Copycat servers.
   */
  private List<CopycatServer> createServers(int nodes) throws Throwable {
    List<CopycatServer> servers = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextMember());
    }

    for (int i = 0; i < nodes; i++) {
      CopycatServer server = createServer(members, members.get(i));
      server.start().thenRun(this::resume);
      servers.add(server);
    }

    await(30000 * nodes, nodes);

    return servers;
  }

  /**
   * Creates a set of Copycat servers.
   */
  private List<CopycatServer> createServers(int live, int total) throws Throwable {
    List<CopycatServer> servers = new ArrayList<>();

    for (int i = 0; i < total; i++) {
      members.add(nextMember());
    }

    for (int i = 0; i < live; i++) {
      CopycatServer server = createServer(members, members.get(i));
      server.start().thenRun(this::resume);
      servers.add(server);
    }

    await(30000 * live, live);

    return servers;
  }

  /**
   * Creates a Copycat server.
   */
  private CopycatServer createServer(List<Member> members, Member member) {
    CopycatServer.Builder builder = CopycatServer.builder(member.clientAddress(), member.serverAddress(), members.stream().map(Member::serverAddress).collect(Collectors.toList()))
      .withTransport(new LocalTransport(registry))
      .withStorage(Storage.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .withMaxSegmentSize(1024 * 1024)
        .withCompactionThreads(1)
        .build())
      .withStateMachine(TestStateMachine::new);

    if (member.type() != Member.Type.INACTIVE) {
      builder.withType(member.type());
    }

    CopycatServer server = builder.build();
    server.serializer().disableWhitelist();
    servers.add(server);
    return server;
  }

  /**
   * Creates a Copycat client.
   */
  private CopycatClient createClient() throws Throwable {
    CopycatClient client = CopycatClient.builder(members.stream().map(Member::clientAddress).collect(Collectors.toList()))
      .withTransport(new LocalTransport(registry))
      .withConnectionStrategy(ConnectionStrategies.FIBONACCI_BACKOFF)
      .withRetryStrategy(RetryStrategies.FIBONACCI_BACKOFF)
      .build();
    client.serializer().disableWhitelist();
    client.connect().thenRun(this::resume);
    await(30000);
    clients.add(client);
    return client;
  }

  @BeforeMethod
  @AfterMethod
  public void clearTests() throws Exception {
    clients.forEach(c -> {
      try {
        c.close().join();
      } catch (Exception e) {
      }
    });

    servers.forEach(s -> {
      try {
        if (s.isRunning()) {
          s.kill().join();
          s.delete().join();
        }
      } catch (Exception e) {
      }
    });

    members = new ArrayList<>();
    port = 5000;
    registry = new LocalServerRegistry();
    clients = new ArrayList<>();
    servers = new ArrayList<>();
  }

  /**
   * Test state machine.
   */
  public static class TestStateMachine extends StateMachine implements SessionListener, Snapshottable {
    private Commit<TestCommand> last;
    private Commit<TestExpire> expire;
    private Commit<TestClose> close;

    @Override
    public void register(ServerSession session) {

    }

    @Override
    public void unregister(ServerSession session) {

    }

    @Override
    public void expire(ServerSession session) {
      if (expire != null)
        expire.session().publish("expired");
    }

    @Override
    public void close(ServerSession session) {
      if (close != null && !session.equals(close.session()))
        close.session().publish("closed");
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
      writer.writeLong(10);
    }

    @Override
    public void install(SnapshotReader reader) {
      assert reader.readLong() == 10;
    }

    public String command(Commit<TestCommand> commit) {
      try {
        return commit.operation().value();
      } finally {
        if (last != null)
          last.close();
        last = commit;
      }
    }

    public String query(Commit<TestQuery> commit) {
      try {
        return commit.operation().value();
      } finally {
        commit.close();
      }
    }

    public String event(Commit<TestEvent> commit) {
      try {
        if (commit.operation().own()) {
          commit.session().publish("test", commit.operation().value());
        } else {
          for (ServerSession session : sessions) {
            session.publish("test", commit.operation().value());
          }
        }
        return commit.operation().value();
      } finally {
        commit.close();
      }
    }

    public void close(Commit<TestClose> commit) {
      this.close = commit;
    }

    public void expire(Commit<TestExpire> commit) {
      this.expire = commit;
    }
  }

  /**
   * Test command.
   */
  public static class TestCommand implements Command<String> {
    private String value;
    private ConsistencyLevel consistency;

    public TestCommand(String value, ConsistencyLevel consistency) {
      this.value = value;
      this.consistency = consistency;
    }

    @Override
    public ConsistencyLevel consistency() {
      return consistency;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }

    public String value() {
      return value;
    }
  }

  /**
   * Test query.
   */
  public static class TestQuery implements Query<String> {
    private String value;
    private ConsistencyLevel consistency;

    public TestQuery(String value, ConsistencyLevel consistency) {
      this.value = value;
      this.consistency = consistency;
    }

    @Override
    public ConsistencyLevel consistency() {
      return consistency;
    }

    public String value() {
      return value;
    }
  }

  /**
   * Test event.
   */
  public static class TestEvent implements Command<String> {
    private String value;
    private boolean own;
    private ConsistencyLevel consistency;

    public TestEvent(String value, boolean own, ConsistencyLevel consistency) {
      this.value = value;
      this.own = own;
      this.consistency = consistency;
    }

    @Override
    public Command.ConsistencyLevel consistency() {
      return consistency;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }

    public String value() {
      return value;
    }

    public boolean own() {
      return own;
    }
  }

  /**
   * Test event.
   */
  public static class TestExpire implements Command<Void> {
  }

  /**
   * Test event.
   */
  public static class TestClose implements Command<Void> {
  }

  /**
   * Test member.
   */
  public static class TestMember implements Member, Serializable {
    private Type type;
    private Address serverAddress;
    private Address clientAddress;

    public TestMember() {
    }

    public TestMember(Type type, Address serverAddress, Address clientAddress) {
      this.type = type;
      this.serverAddress = serverAddress;
      this.clientAddress = clientAddress;
    }

    @Override
    public int id() {
      return serverAddress.hashCode();
    }

    @Override
    public Address address() {
      return serverAddress;
    }

    @Override
    public Address clientAddress() {
      return clientAddress;
    }

    @Override
    public Address serverAddress() {
      return serverAddress;
    }

    @Override
    public Type type() {
      return type;
    }

    @Override
    public Listener<Type> onTypeChange(Consumer<Type> callback) {
      return null;
    }

    @Override
    public Status status() {
      return null;
    }

    @Override
    public Instant updated() {
      return null;
    }

    @Override
    public Listener<Status> onStatusChange(Consumer<Status> callback) {
      return null;
    }

    @Override
    public CompletableFuture<Void> promote() {
      return null;
    }

    @Override
    public CompletableFuture<Void> promote(Type type) {
      return null;
    }

    @Override
    public CompletableFuture<Void> demote() {
      return null;
    }

    @Override
    public CompletableFuture<Void> demote(Type type) {
      return null;
    }

    @Override
    public CompletableFuture<Void> remove() {
      return null;
    }
  }

}
