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
import io.atomix.catalyst.transport.local.LocalServerRegistry;
import io.atomix.catalyst.transport.local.LocalTransport;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.DefaultCopycatClient;
import io.atomix.copycat.client.RecoveryStrategies;
import io.atomix.copycat.client.RecoveryStrategy;
import io.atomix.copycat.client.session.ClientSession;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
    server.bootstrap().thenRun(this::resume);
    await(5000);
    CopycatServer joiner1 = createServer(nextMember(Member.Type.ACTIVE));
    joiner1.join(server.cluster().member().address()).thenRun(this::resume);
    await(5000);
    CopycatServer joiner2 = createServer(nextMember(Member.Type.ACTIVE));
    joiner2.join(server.cluster().member().address()).thenRun(this::resume);
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
    CopycatServer joiner = createServer(nextMember(type));
    joiner.onStateChange(s -> {
      if (s == state)
        resume();
    });
    joiner.join(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(this::resume);
    await(30000, 2);
  }

  /**
   * Submits a bunch of commands recursively.
   */
  private void submit(CopycatClient client, int count, int total) {
    if (count < total) {
      client.submit(new TestCommand()).whenComplete((result, error) -> {
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
    servers.get(0).shutdown().get(10, TimeUnit.SECONDS);
    CopycatServer server = createServer(members.get(0));
    server.join(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(this::resume);
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
    server.leave().thenRun(this::resume);
    await(30000);
  }

  /**
   * Tests leaving the leader from a cluster.
   */
  public void testLeaderLeave() throws Throwable {
    List<CopycatServer> servers = createServers(3);
    CopycatServer server = servers.stream().filter(s -> s.state() == CopycatServer.State.LEADER).findFirst().get();
    server.leave().thenRun(this::resume);
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
    CopycatServer server = createServer(nextMember(type));
    server.join(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(this::resume);
    await(10000);
  }

  /**
   * Tests joining and leaving the cluster, resizing the quorum.
   */
  public void testResize() throws Throwable {
    CopycatServer server = createServers(1).get(0);
    CopycatServer joiner = createServer(nextMember(Member.Type.ACTIVE));
    joiner.join(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(this::resume);
    await(10000);
    server.leave().thenRun(this::resume);
    await(10000);
    joiner.leave().thenRun(this::resume);
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
    CopycatServer joiner = createServer(member);
    joiner.join(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(this::resume);
    await(10000);

    joiner.shutdown().thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests detecting an availability change of a reserve member on a passive member.
   */
  public void testPassiveReserveAvailabilityChange() throws Throwable {
    createServers(3);

    CopycatServer passive = createServer(nextMember(Member.Type.PASSIVE));
    passive.join(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(this::resume);

    await(10000);

    Member reserveMember = nextMember(Member.Type.RESERVE);
    passive.cluster().onJoin(member -> {
      threadAssertEquals(member.address(), reserveMember.address());
      member.onStatusChange(s -> {
        threadAssertEquals(s, Member.Status.UNAVAILABLE);
        resume();
      });
    });

    CopycatServer reserve = createServer(reserveMember);
    reserve.join(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(this::resume);

    await(10000);

    reserve.shutdown().thenRun(this::resume);
    await(10000, 2);
  }

  /**
   * Tests detecting an availability change of a passive member on a reserve member.
   */
  public void testReservePassiveAvailabilityChange() throws Throwable {
    createServers(3);

    CopycatServer passive = createServer(nextMember(Member.Type.PASSIVE));
    passive.join(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(this::resume);

    CopycatServer reserve = createServer(nextMember(Member.Type.RESERVE));
    reserve.join(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(this::resume);

    await(10000, 2);

    reserve.cluster().member(passive.cluster().member().address()).onStatusChange(s -> {
      threadAssertEquals(s, Member.Status.UNAVAILABLE);
      resume();
    });

    passive.shutdown().thenRun(this::resume);
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

    CopycatServer joiner = createServer(member);
    joiner.join(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(this::resume);
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
  public void testOneNodeSubmitCommand() throws Throwable {
    testSubmitCommand(1);
  }

  /**
   * Tests submitting a command.
   */
  public void testTwoNodeSubmitCommand() throws Throwable {
    testSubmitCommand(2);
  }

  /**
   * Tests submitting a command.
   */
  public void testThreeNodeSubmitCommand() throws Throwable {
    testSubmitCommand(3);
  }

  /**
   * Tests submitting a command.
   */
  public void testFourNodeSubmitCommand() throws Throwable {
    testSubmitCommand(4);
  }

  /**
   * Tests submitting a command.
   */
  public void testFiveNodeSubmitCommand() throws Throwable {
    testSubmitCommand(5);
  }

  /**
   * Tests submitting a command with a configured consistency level.
   */
  private void testSubmitCommand(int nodes) throws Throwable {
    createServers(nodes);

    CopycatClient client = createClient();
    client.submit(new TestCommand()).thenAccept(result -> {
      threadAssertNotNull(result);
      resume();
    });

    await(30000);
  }

  /**
   * Tests submitting a command.
   */
  public void testTwoOfThreeNodeSubmitCommand() throws Throwable {
    testSubmitCommand(2, 3);
  }

  /**
   * Tests submitting a command.
   */
  public void testThreeOfFourNodeSubmitCommand() throws Throwable {
    testSubmitCommand(3, 4);
  }

  /**
   * Tests submitting a command.
   */
  public void testThreeOfFiveNodeSubmitCommand() throws Throwable {
    testSubmitCommand(3, 5);
  }

  /**
   * Tests submitting a command to a partial cluster.
   */
  private void testSubmitCommand(int live, int total) throws Throwable {
    createServers(live, total);

    CopycatClient client = createClient();
    client.submit(new TestCommand()).thenAccept(result -> {
      threadAssertNotNull(result);
      resume();
    });

    await(30000);
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
    testSubmitQuery(1, Query.ConsistencyLevel.LINEARIZABLE_LEASE);
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
  public void testTwoNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(2, Query.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testTwoNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(2, Query.ConsistencyLevel.LINEARIZABLE_LEASE);
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
  public void testThreeNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(3, Query.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testThreeNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(3, Query.ConsistencyLevel.LINEARIZABLE_LEASE);
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
  public void testFourNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(4, Query.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testFourNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(4, Query.ConsistencyLevel.LINEARIZABLE_LEASE);
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
  public void testFiveNodeSubmitQueryWithSequentialConsistency() throws Throwable {
    testSubmitQuery(5, Query.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a query.
   */
  public void testFiveNodeSubmitQueryWithBoundedLinearizableConsistency() throws Throwable {
    testSubmitQuery(5, Query.ConsistencyLevel.LINEARIZABLE_LEASE);
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
    client.submit(new TestQuery(consistency)).thenAccept(result -> {
      threadAssertNotNull(result);
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

    AtomicLong count = new AtomicLong();
    AtomicLong index = new AtomicLong();

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertEquals(count.incrementAndGet(), 2L);
      threadAssertEquals(index.get(), message);
      resume();
    });

    client.submit(new TestEvent(true)).thenAccept(result -> {
      threadAssertNotNull(result);
      threadAssertEquals(count.incrementAndGet(), 1L);
      index.set(result);
      resume();
    });

    await(30000, 2);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testOneNodeEvents() throws Throwable {
    testEvents(1);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testTwoNodeEvents() throws Throwable {
    testEvents(2);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testThreeNodeEvents() throws Throwable {
    testEvents(3);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testFourNodeEvents() throws Throwable {
    testEvents(4);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testFiveNodeEvents() throws Throwable {
    testEvents(5);
  }

  /**
   * Tests submitting sequential events to all sessions.
   */
  private void testEvents(int nodes) throws Throwable {
    createServers(nodes);

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertNotNull(message);
      resume();
    });
    createClient().onEvent("test", message -> {
      threadAssertNotNull(message);
      resume();
    });
    createClient().onEvent("test", message -> {
      threadAssertNotNull(message);
      resume();
    });

    client.submit(new TestEvent(false)).thenAccept(result -> {
      threadAssertNotNull(result);
      resume();
    });

    await(30000, 4);
  }

  /**
   * Tests that operations are properly sequenced on the client.
   */
  public void testSequenceLinearizableOperations() throws Throwable {
    testSequenceOperations(5, Query.ConsistencyLevel.LINEARIZABLE);
  }

  /**
   * Tests that operations are properly sequenced on the client.
   */
  public void testSequenceBoundedLinearizableOperations() throws Throwable {
    testSequenceOperations(5, Query.ConsistencyLevel.LINEARIZABLE_LEASE);
  }

  /**
   * Tests that operations are properly sequenced on the client.
   */
  public void testSequenceSequentialOperations() throws Throwable {
    testSequenceOperations(5, Query.ConsistencyLevel.SEQUENTIAL);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testSequenceOperations(int nodes, Query.ConsistencyLevel consistency) throws Throwable {
    createServers(nodes);

    AtomicInteger counter = new AtomicInteger();
    AtomicLong index = new AtomicLong();

    CopycatClient client = createClient();
    client.<Long>onEvent("test", message -> {
      threadAssertEquals(counter.incrementAndGet(), 3);
      threadAssertTrue(message >= index.get());
      index.set(message);
      resume();
    });

    client.submit(new TestCommand()).thenAccept(result -> {
      threadAssertNotNull(result);
      threadAssertEquals(counter.incrementAndGet(), 1);
      threadAssertTrue(index.compareAndSet(0, result));
      resume();
    });

    client.submit(new TestEvent(true)).thenAccept(result -> {
      threadAssertNotNull(result);
      threadAssertEquals(counter.incrementAndGet(), 2);
      threadAssertTrue(result > index.get());
      index.set(result);
      resume();
    });

    client.submit(new TestQuery(consistency)).thenAccept(result -> {
      threadAssertNotNull(result);
      threadAssertEquals(counter.incrementAndGet(), 4);
      long i = index.get();
      threadAssertTrue(result >= i);
      resume();
    });

    await(30000, 4);
  }

  /**
   * Tests blocking within an event thread.
   */
  public void testBlockOnEvent() throws Throwable {
    createServers(3);

    AtomicLong index = new AtomicLong();

    CopycatClient client = createClient();

    client.onEvent("test", event -> {
      threadAssertEquals(index.get(), event);
      try {
        threadAssertTrue(index.get() <= client.submit(new TestQuery(Query.ConsistencyLevel.LINEARIZABLE)).get(10, TimeUnit.SECONDS));
      } catch (InterruptedException | TimeoutException | ExecutionException e) {
        threadFail(e);
      }
      resume();
    });

    client.submit(new TestEvent(true)).thenAccept(result -> {
      threadAssertNotNull(result);
      index.compareAndSet(0, result);
      resume();
    });

    await(10000, 2);
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

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertNotNull(message);
      resume();
    });

    for (int i = 0 ; i < 10; i++) {
      client.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
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

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertNotNull(message);
      resume();
    });

    for (int i = 0; i < 10; i++) {
      client.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
        resume();
      });

      await(30000, 2);
    }

    CopycatServer leader = servers.stream().filter(s -> s.state() == CopycatServer.State.LEADER).findFirst().get();
    leader.shutdown().get(10, TimeUnit.SECONDS);

    for (int i = 0; i < 10; i++) {
      client.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
        resume();
      });

      await(30000, 2);
    }
  }

  /**
   * Tests submitting sequential events.
   */
  public void testThreeNodesEventsAfterFollowerKill() throws Throwable {
    testEventsAfterFollowerKill(3);
  }

  /**
   * Tests submitting sequential events.
   */
  public void testFiveNodesEventsAfterFollowerKill() throws Throwable {
    testEventsAfterFollowerKill(5);
  }

  /**
   * Tests submitting a sequential event that publishes to all sessions.
   */
  private void testEventsAfterFollowerKill(int nodes) throws Throwable {
    List<CopycatServer> servers = createServers(nodes);

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertNotNull(message);
      resume();
    });

    for (int i = 0 ; i < 10; i++) {
      client.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
        resume();
      });

      await(30000, 2);
    }

    client.submit(new TestEvent(true)).thenAccept(result -> {
      threadAssertNotNull(result);
      resume();
    });

    CopycatServer follower = servers.stream().filter(s -> s.state() == CopycatServer.State.FOLLOWER).findFirst().get();
    follower.shutdown().get(10, TimeUnit.SECONDS);

    await(30000, 2);

    for (int i = 0 ; i < 10; i++) {
      client.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
        resume();
      });

      await(30000, 2);
    }
  }

  /**
   * Tests submitting events.
   */
  public void testFiveNodesEventsAfterLeaderKill() throws Throwable {
    testEventsAfterLeaderKill(5);
  }

  /**
   * Tests submitting a linearizable event that publishes to all sessions.
   */
  private void testEventsAfterLeaderKill(int nodes) throws Throwable {
    List<CopycatServer> servers = createServers(nodes);

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertNotNull(message);
      resume();
    });

    for (int i = 0 ; i < 10; i++) {
      client.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
        resume();
      });

      await(30000, 2);
    }

    client.submit(new TestEvent(true)).thenAccept(result -> {
      threadAssertNotNull(result);
      resume();
    });

    CopycatServer leader = servers.stream().filter(s -> s.state() == CopycatServer.State.LEADER).findFirst().get();
    leader.shutdown().get(10, TimeUnit.SECONDS);

    await(30000, 2);

    for (int i = 0 ; i < 10; i++) {
      client.submit(new TestEvent(true)).thenAccept(result -> {
        threadAssertNotNull(result);
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

    CopycatClient client = createClient();
    client.onEvent("test", message -> {
      threadAssertNotNull(message);
      resume();
    });

    createClient().onEvent("test", message -> {
      threadAssertNotNull(message);
      resume();
    });

    createClient().onEvent("test", message -> {
      threadAssertNotNull(message);
      resume();
    });

    for (int i = 0; i < 10; i++) {
      client.submit(new TestEvent(false)).thenAccept(result -> {
        threadAssertNotNull(result);
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
   * Tests state transition with recovery
   */
  public void testStateTransitionWithRecovery() throws Throwable {
    createServers(3);
    final CopycatClient client = createClient(RecoveryStrategies.RECOVER);
    final AtomicReference<CopycatClient.State> prev =
        new AtomicReference<>(CopycatClient.State.CONNECTED);
    Listener<CopycatClient.State> stateListener = client.onStateChange(s -> {
      switch (s) {
        case CONNECTED:
          threadAssertEquals(CopycatClient.State.SUSPENDED,
              prev.getAndSet(CopycatClient.State.CONNECTED));
          resume();
          break;
        case SUSPENDED:
          threadAssertEquals(CopycatClient.State.CONNECTED,
              prev.getAndSet(CopycatClient.State.SUSPENDED));
          resume();
          break;
        case CLOSED:
          threadFail("State not allowed");
      }
    });
    ((ClientSession) client.session()).expire().thenAccept(v -> resume());
    await(5000, 3);
    stateListener.close();
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
      members.add(nextMember(Member.Type.ACTIVE));
    }

    for (int i = 0; i < nodes; i++) {
      CopycatServer server = createServer(members.get(i));
      server.bootstrap(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(this::resume);
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
      members.add(nextMember(Member.Type.ACTIVE));
    }

    for (int i = 0; i < live; i++) {
      CopycatServer server = createServer(members.get(i));
      server.bootstrap(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(this::resume);
      servers.add(server);
    }

    await(30000 * live, live);

    return servers;
  }

  /**
   * Creates a Copycat server.
   */
  private CopycatServer createServer(Member member) {
    CopycatServer.Builder builder = CopycatServer.builder(member.clientAddress(), member.serverAddress())
      .withType(member.type())
      .withTransport(new LocalTransport(registry))
      .withStorage(Storage.builder()
        .withStorageLevel(StorageLevel.MEMORY)
        .withMaxSegmentSize(1024 * 1024)
        .withCompactionThreads(1)
        .build())
      .withStateMachine(TestStateMachine::new);

    CopycatServer server = builder.build();
    server.serializer().disableWhitelist();
    servers.add(server);
    return server;
  }

  /**
   * Creates a Copycat client.
   */
  private CopycatClient createClient() throws Throwable {
    return createClient(RecoveryStrategies.CLOSE);
  }

  /**
   * Creates a Copycat client.
   */
  private CopycatClient createClient(RecoveryStrategy strategy) throws Throwable {
    CopycatClient client = CopycatClient.builder()
      .withTransport(new LocalTransport(registry))
      .withConnectionStrategy(ConnectionStrategies.FIBONACCI_BACKOFF)
      .withRecoveryStrategy(strategy)
      .build();
    client.serializer().disableWhitelist();
    client.connect(members.stream().map(Member::clientAddress).collect(Collectors.toList())).thenRun(this::resume);
    await(30000);
    clients.add(client);
    return client;
  }

  @BeforeMethod
  @AfterMethod
  public void clearTests() throws Exception {
    clients.forEach(c -> {
      try {
        c.close().get(10, TimeUnit.SECONDS);
      } catch (Exception e) {
      }
    });

    servers.forEach(s -> {
      try {
        if (s.isRunning()) {
          s.shutdown().get(10, TimeUnit.SECONDS);
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

    public long command(Commit<TestCommand> commit) {
      try {
        return commit.index();
      } finally {
        if (last != null)
          last.close();
        last = commit;
      }
    }

    public long query(Commit<TestQuery> commit) {
      try {
        return commit.index();
      } finally {
        commit.close();
      }
    }

    public long event(Commit<TestEvent> commit) {
      try {
        if (commit.operation().own()) {
          commit.session().publish("test", commit.index());
        } else {
          for (ServerSession session : sessions) {
            session.publish("test", commit.index());
          }
        }
        return commit.index();
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
  public static class TestCommand implements Command<Long> {
    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }
  }

  /**
   * Test query.
   */
  public static class TestQuery implements Query<Long> {
    private ConsistencyLevel consistency;

    public TestQuery(ConsistencyLevel consistency) {
      this.consistency = consistency;
    }

    @Override
    public ConsistencyLevel consistency() {
      return consistency;
    }
  }

  /**
   * Test event.
   */
  public static class TestEvent implements Command<Long> {
    private boolean own;

    public TestEvent(boolean own) {
      this.own = own;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
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
