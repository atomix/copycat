/*
 * Copyright 2017 the original author or authors.
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

import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.catalyst.concurrent.ThreadContext;
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
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Copycat fuzz test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class FuzzTest implements Runnable {

  /**
   * Runs the test.
   */
  public static void main(String[] args) {
    new FuzzTest().run();
  }

  private static final int ITERATIONS = 1000;
  private static final String CHARS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

  private int port = 5000;
  private List<Member> members = new ArrayList<>();
  private List<CopycatClient> clients = new ArrayList<>();
  private List<CopycatServer> servers = new ArrayList<>();
  private Map<Integer, Scheduled> shutdownTimers = new ConcurrentHashMap<>();
  private Map<Integer, Scheduled> restartTimers = new ConcurrentHashMap<>();
  private static final String[] KEYS = new String[1024];
  private final Random random = new Random();

  static {
    for (int i = 0; i < 1024; i++) {
      KEYS[i] = UUID.randomUUID().toString();
    }
  }

  @Override
  public void run() {
    for (int i = 0; i < ITERATIONS; i++) {
      try {
        runFuzzTest();
      } catch (Exception e) {
        e.printStackTrace();
        return;
      }
    }
  }

  /**
   * Returns a random map key.
   */
  private String randomKey() {
    return KEYS[randomNumber(KEYS.length)];
  }

  /**
   * Returns a random query consistency level.
   */
  private Query.ConsistencyLevel randomConsistency() {
    return Query.ConsistencyLevel.values()[randomNumber(Query.ConsistencyLevel.values().length)];
  }

  /**
   * Returns a random number within the given range.
   */
  private int randomNumber(int limit) {
    return random.nextInt(limit);
  }

  /**
   * Returns a random boolean.
   */
  private boolean randomBoolean() {
    return randomNumber(2) == 1;
  }

  /**
   * Returns a random string up to the given length.
   */
  private String randomString(int maxLength) {
    int length = randomNumber(maxLength) + 1;
    StringBuilder sb = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
      sb.append(CHARS.charAt(random.nextInt(CHARS.length())));
    }
    return sb.toString();
  }

  /**
   * Runs a single fuzz test.
   */
  private void runFuzzTest() throws Exception {
    reset();

    createServers(randomNumber(5) + 3);

    final Object lock = new Object();
    final AtomicLong index = new AtomicLong();
    final Map<Integer, Long> indexes = new HashMap<>();

    int clients = randomNumber(100) + 1;
    for (int i = 0; i < clients; i++) {
      CopycatClient client = createClient(RecoveryStrategies.RECOVER);

      final int clientId = i;
      client.context().schedule(Duration.ofMillis((100 * clients) + (randomNumber(50) - 25)), Duration.ofMillis((100 * clients) + (randomNumber(50) - 25)), () -> {
        long lastLinearizableIndex = index.get();
        int type = randomNumber(4);
        switch (type) {
          case 0:
            client.submit(new Put(randomKey(), randomString(1024 * 16))).thenAccept(result -> {
              synchronized (lock) {
                if (result < lastLinearizableIndex) {
                  System.out.println(result + " is less than last linearizable index " + lastLinearizableIndex);
                  System.exit(1);
                } else if (result > index.get()) {
                  index.set(result);
                }

                Long lastSequentialIndex = indexes.get(clientId);
                if (lastSequentialIndex == null) {
                  indexes.put(clientId, result);
                } else if (result < lastSequentialIndex) {
                  System.out.println(result + " is less than last sequential index " + lastSequentialIndex);
                  System.exit(1);
                } else {
                  indexes.put(clientId, lastSequentialIndex);
                }
              }
            });
            break;
          case 1:
            client.submit(new Get(randomKey(), randomConsistency()));
            break;
          case 2:
            client.submit(new Remove(randomKey())).thenAccept(result -> {
              synchronized (lock) {
                if (result < lastLinearizableIndex) {
                  System.out.println(result + " is less than last linearizable index " + lastLinearizableIndex);
                  System.exit(1);
                } else if (result > index.get()) {
                  index.set(result);
                }

                Long lastSequentialIndex = indexes.get(clientId);
                if (lastSequentialIndex == null) {
                  indexes.put(clientId, result);
                } else if (result < lastSequentialIndex) {
                  System.out.println(result + " is less than last sequential index " + lastSequentialIndex);
                  System.exit(1);
                } else {
                  indexes.put(clientId, lastSequentialIndex);
                }
              }
            });
            break;
          case 3:
            Query.ConsistencyLevel consistency = randomConsistency();
            client.submit(new Index(consistency)).thenAccept(result -> {
              synchronized (lock) {
                switch (consistency) {
                  case LINEARIZABLE:
                  case LINEARIZABLE_LEASE:
                    if (result < lastLinearizableIndex) {
                      System.out.println(result + " is less than last linearizable index " + lastLinearizableIndex);
                      System.exit(1);
                    } else if (result > index.get()) {
                      index.set(result);
                    }
                  case SEQUENTIAL:
                    Long lastSequentialIndex = indexes.get(clientId);
                    if (lastSequentialIndex == null) {
                      indexes.put(clientId, result);
                    } else if (result < lastSequentialIndex) {
                      System.out.println(result + " is less than last sequential index " + lastSequentialIndex);
                      System.exit(1);
                    } else {
                      indexes.put(clientId, lastSequentialIndex);
                    }
                }
              }
            });
        }
      });
    }

    ThreadContext context = this.clients.get(0).context();
    scheduleRestarts(context);

    Thread.sleep(Duration.ofMinutes(15).toMillis());
  }

  /**
   * Schedules a random number of servers to be shutdown for a period of time and then restarted.
   */
  private void scheduleRestarts(ThreadContext context) {
    if (shutdownTimers.isEmpty() && restartTimers.isEmpty()) {
      int shutdownCount = randomNumber(servers.size() - 2) + 1;
      boolean remove = randomBoolean();
      for (int i = 0; i < shutdownCount; i++) {
        scheduleRestart(remove, i, context);
      }
    }
  }

  /**
   * Schedules the given server to be shutdown for a period of time and then restarted.
   */
  private void scheduleRestart(boolean remove, int serverIndex, ThreadContext context) {
    shutdownTimers.put(serverIndex, context.schedule(Duration.ofSeconds(randomNumber(120) + 10), () -> {
      shutdownTimers.remove(serverIndex);
      CopycatServer server = servers.get(serverIndex);
      CompletableFuture<Void> leaveFuture;
      if (remove) {
        System.out.println("Removing server: " + server.cluster().member().address());
        leaveFuture = server.leave();
      } else {
        System.out.println("Shutting down server: " + server.cluster().member().address());
        leaveFuture = server.shutdown();
      }
      leaveFuture.whenComplete((result, error) -> {
        restartTimers.put(serverIndex, context.schedule(Duration.ofSeconds(randomNumber(120) + 10), () -> {
          restartTimers.remove(serverIndex);
          CopycatServer newServer = createServer(server.cluster().member());
          servers.set(serverIndex, newServer);
          CompletableFuture<CopycatServer> joinFuture;
          if (remove) {
            System.out.println("Adding server: " + newServer.cluster().member().address());
            joinFuture = newServer.join(members.get(members.size() - 1).address());
          } else {
            System.out.println("Bootstrapping server: " + newServer.cluster().member().address());
            joinFuture = newServer.bootstrap(members.stream().map(Member::serverAddress).collect(Collectors.toList()));
          }
          joinFuture.whenComplete((result2, error2) -> {
            scheduleRestarts(context);
          });
        }));
      });
    }));
  }

  /**
   * Resets the test state.
   */
  private void reset() throws Exception {
    for (Scheduled shutdownTimer : shutdownTimers.values()) {
      shutdownTimer.cancel();
    }
    shutdownTimers.clear();

    for (Scheduled restartTimer : restartTimers.values()) {
      restartTimer.cancel();
    }
    restartTimers.clear();

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

    Path directory = Paths.get("target/fuzz-logs/");
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

    members = new ArrayList<>();
    port = 5000;
    clients = new ArrayList<>();
    servers = new ArrayList<>();
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
        .withDirectory(new File(String.format("target/fuzz-logs/%d", member.address().hashCode())))
        .withMaxSegmentSize(randomNumber(1024 * 1024 * 7) + (1024 * 1024))
        .withMaxEntriesPerSegment(randomNumber(10000) + 1000)
        .withCompactionThreads(randomNumber(4) + 1)
        .withCompactionThreshold(Math.random() / (double) 2)
        .withEntryBufferSize(randomNumber(10000) + 1)
        .withFlushOnCommit(randomBoolean())
        .withMinorCompactionInterval(Duration.ofSeconds(randomNumber(30) + 15))
        .withMajorCompactionInterval(Duration.ofSeconds(randomNumber(60) + 60))
        .build())
      .withStateMachine(FuzzStateMachine::new);

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
      .withServerSelectionStrategy(ServerSelectionStrategies.values()[randomNumber(ServerSelectionStrategies.values().length)])
      .build();
    client.serializer().disableWhitelist();
    CountDownLatch latch = new CountDownLatch(1);
    client.connect(members.stream().map(Member::clientAddress).collect(Collectors.toList())).thenRun(latch::countDown);
    latch.await(30, TimeUnit.SECONDS);
    clients.add(client);
    return client;
  }

  /**
   * Fuzz test state machine.
   */
  public class FuzzStateMachine extends StateMachine implements SessionListener, Snapshottable {
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
      writer.writeString(randomString(1024 * 1024 * 8));
    }

    @Override
    public void install(SnapshotReader reader) {
      String string = reader.readString();
      assert string != null;
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
