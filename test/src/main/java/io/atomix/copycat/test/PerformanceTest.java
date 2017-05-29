/*
 * Copyright 2017-present the original author or authors.
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
package io.atomix.copycat.test;

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.Command;
import io.atomix.copycat.Query;
import io.atomix.copycat.client.*;
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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Copycat performance test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class PerformanceTest implements Runnable {

  /**
   * Runs the test.
   */
  public static void main(String[] args) {
    new PerformanceTest().run();
  }

  private static final int ITERATIONS = 10;

  private static final int TOTAL_OPERATIONS = 100000;
  private static final int WRITE_RATIO = 5;
  private static final int NUM_CLIENTS = 5;

  private static final Query.ConsistencyLevel QUERY_CONSISTENCY = Query.ConsistencyLevel.LINEARIZABLE;
  private static final ServerSelectionStrategy SERVER_SELECTION_STRATEGY = ServerSelectionStrategies.ANY;

  private int port = 5000;
  private List<Member> members = new ArrayList<>();
  private List<CopycatClient> clients = new ArrayList<>();
  private List<CopycatServer> servers = new ArrayList<>();
  private static final String[] KEYS = new String[1024];
  private final Random random = new Random();
  private final List<Long> iterations = new ArrayList<>();
  private final AtomicInteger totalOperations = new AtomicInteger();
  private final AtomicInteger writeCount = new AtomicInteger();
  private final AtomicInteger readCount = new AtomicInteger();

  static {
    for (int i = 0; i < 1024; i++) {
      KEYS[i] = UUID.randomUUID().toString();
    }
  }

  @Override
  public void run() {
    for (int i = 0; i < ITERATIONS; i++) {
      try {
        iterations.add(runIteration());
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
    }

    System.out.println("Completed " + ITERATIONS + " iterations");
    long averageRunTime = (long) iterations.stream().mapToLong(v -> v).average().getAsDouble();
    System.out.println(String.format("averageRunTime: %dms", averageRunTime));

    try {
      shutdown();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Runs a single performance test iteration, returning the iteration run time.
   */
  private long runIteration() throws Exception {
    reset();

    createServers(3);

    CompletableFuture<Void>[] futures = new CompletableFuture[NUM_CLIENTS];
    CopycatClient[] clients = new CopycatClient[NUM_CLIENTS];
    for (int i = 0; i < NUM_CLIENTS; i++) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      clients[i] = createClient(RecoveryStrategies.RECOVER);
      futures[i] = future;
    }

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < clients.length; i++) {
      runClient(clients[i], futures[i]);
    }
    CompletableFuture.allOf(futures).join();
    long endTime = System.currentTimeMillis();
    long runTime = endTime - startTime;
    System.out.println(String.format("readCount: %d/%d, writeCount: %d/%d, runTime: %dms",
      readCount.get(),
      (int) (TOTAL_OPERATIONS * (WRITE_RATIO / 10d)),
      writeCount.get(),
      (int) (TOTAL_OPERATIONS * (1 - (WRITE_RATIO / 10d))),
      runTime));
    return runTime;
  }

  /**
   * Runs operations for a single client.
   */
  private void runClient(CopycatClient client, CompletableFuture<Void> future) {
    int count = totalOperations.incrementAndGet();
    if (count > TOTAL_OPERATIONS) {
      future.complete(null);
    } else if (count % 10 < WRITE_RATIO) {
      client.submit(new Put(randomKey(), UUID.randomUUID().toString())).whenComplete((result, error) -> {
        if (error == null) {
          writeCount.incrementAndGet();
        }
        runClient(client, future);
      });
    } else {
      client.submit(new Get(randomKey(), QUERY_CONSISTENCY)).whenComplete((result, error) -> {
        if (error == null) {
          readCount.incrementAndGet();
        }
        runClient(client, future);
      });
    }
  }

  /**
   * Resets the test state.
   */
  private void reset() throws Exception {
    totalOperations.set(0);
    readCount.set(0);
    writeCount.set(0);

    shutdown();

    members = new ArrayList<>();
    port = 5000;
    clients = new ArrayList<>();
    servers = new ArrayList<>();
  }

  /**
   * Shuts down clients and servers.
   */
  private void shutdown() throws Exception {
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

    Path directory = Paths.get("target/performance-logs/");
    if (Files.exists(directory)) {
      Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
          Files.delete(file);
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
          Files.delete(dir);
          return FileVisitResult.CONTINUE;
        }
      });
    }
  }

  /**
   * Returns a random map key.
   */
  private String randomKey() {
    return KEYS[randomNumber(KEYS.length)];
  }

  /**
   * Returns a random number within the given range.
   */
  private int randomNumber(int limit) {
    return random.nextInt(limit);
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
  private List<CopycatServer> createServers(int nodes) throws Exception {
    List<CopycatServer> servers = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextMember(Member.Type.ACTIVE));
    }

    CountDownLatch latch = new CountDownLatch(nodes);
    for (int i = 0; i < nodes; i++) {
      CopycatServer server = createServer(members.get(i));
      server.bootstrap(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(latch::countDown);
      servers.add(server);
    }

    latch.await(30 * nodes, TimeUnit.SECONDS);

    return servers;
  }

  /**
   * Creates a Copycat server.
   */
  private CopycatServer createServer(Member member) {
    CopycatServer.Builder builder = CopycatServer.builder(member.clientAddress(), member.serverAddress())
      .withType(member.type())
      .withTransport(new NettyTransport())
      .withStorage(Storage.builder()
        .withStorageLevel(StorageLevel.DISK)
        .withDirectory(new File(String.format("target/performance-logs/%d", member.address().hashCode())))
        .withCompactionThreads(1)
        .build())
      .withStateMachine(PerformanceStateMachine::new);

    CopycatServer server = builder.build();
    server.serializer().disableWhitelist();
    servers.add(server);
    return server;
  }

  /**
   * Creates a Copycat client.
   */
  private CopycatClient createClient(RecoveryStrategy strategy) throws Exception {
    CopycatClient client = CopycatClient.builder()
      .withTransport(new NettyTransport())
      .withConnectionStrategy(ConnectionStrategies.FIBONACCI_BACKOFF)
      .withRecoveryStrategy(strategy)
      .withServerSelectionStrategy(SERVER_SELECTION_STRATEGY)
      .build();
    client.serializer().disableWhitelist();
    CountDownLatch latch = new CountDownLatch(1);
    client.connect(members.stream().map(Member::clientAddress).collect(Collectors.toList())).thenRun(latch::countDown);
    latch.await(30, TimeUnit.SECONDS);
    clients.add(client);
    return client;
  }

  /**
   * Performance test state machine.
   */
  public class PerformanceStateMachine extends StateMachine implements SessionListener, Snapshottable {
    private Map<String, Commit<Put>> map = new HashMap<>();

    @Override
    public void register(ServerSession session) {

    }

    @Override
    public void unregister(ServerSession session) {

    }

    @Override
    public void expire(ServerSession session) {

    }

    @Override
    public void close(ServerSession session) {

    }

    @Override
    public void snapshot(SnapshotWriter writer) {
    }

    @Override
    public void install(SnapshotReader reader) {
    }

    public long put(Commit<Put> commit) {
      try {
        Commit<Put> old = map.put(commit.operation().key, commit);
        if (old != null) {
          old.close();
        }
        return commit.index();
      } catch (Exception e) {
        commit.close();
        throw e;
      }
    }

    public String get(Commit<Get> commit) {
      try {
        Commit<Put> value = map.get(commit.operation().key);
        return value != null ? value.operation().value : null;
      } finally {
        commit.close();
      }
    }

    public long remove(Commit<Remove> commit) {
      try {
        Commit<Put> removed = map.remove(commit.operation().key);
        if (removed != null) {
          removed.close();
        }
        return commit.index();
      } finally {
        commit.close();
      }
    }

    public long index(Commit<Index> commit) {
      try {
        return commit.index();
      } finally {
        commit.close();
      }
    }
  }

  public static class Put implements Command<Long> {
    public String key;
    public String value;

    public Put(String key, String value) {
      this.key = key;
      this.value = value;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.QUORUM;
    }
  }

  public static class Get implements Query<String> {
    public String key;
    private ConsistencyLevel consistency;

    public Get(String key, ConsistencyLevel consistency) {
      this.key = key;
      this.consistency = consistency;
    }

    @Override
    public ConsistencyLevel consistency() {
      return consistency;
    }
  }

  public static class Index implements Query<Long> {
    private ConsistencyLevel consistency;

    public Index(ConsistencyLevel consistency) {
      this.consistency = consistency;
    }

    @Override
    public ConsistencyLevel consistency() {
      return consistency;
    }
  }

  public static class Remove implements Command<Long> {
    public String key;

    public Remove(String key) {
      this.key = key;
    }

    @Override
    public CompactionMode compaction() {
      return CompactionMode.TOMBSTONE;
    }
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
