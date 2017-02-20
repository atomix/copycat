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

import io.atomix.copycat.ConsistencyLevel;
import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.client.RecoveryStrategies;
import io.atomix.copycat.client.RecoveryStrategy;
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.protocol.tcp.NettyTcpProtocol;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.Snapshottable;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.tcp.NettyTcpRaftProtocol;
import io.atomix.copycat.server.session.Session;
import io.atomix.copycat.server.session.SessionListener;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.copycat.server.storage.snapshot.SnapshotReader;
import io.atomix.copycat.server.storage.snapshot.SnapshotWriter;
import io.atomix.copycat.util.buffer.Buffer;
import io.atomix.copycat.util.buffer.HeapBuffer;
import io.atomix.copycat.util.concurrent.Listener;
import io.atomix.copycat.util.concurrent.Scheduled;
import io.atomix.copycat.util.concurrent.ThreadContext;

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
  private ConsistencyLevel randomConsistency() {
    return ConsistencyLevel.values()[randomNumber(ConsistencyLevel.values().length)];
  }

  /**
   * Returns a random number within the given range.
   */
  private int randomNumber(int limit) {
    return random.nextInt(limit);
  }

  /**
   * Runs a single fuzz test.
   */
  private void runFuzzTest() throws Exception {
    reset();

    createServers(randomNumber(4) + 3);

    int clients = randomNumber(5) + 1;
    for (int i = 0; i < clients; i++) {
      CopycatClient client = createClient(RecoveryStrategies.RECOVER);

      client.context().schedule(Duration.ofMillis(100), Duration.ofMillis(100), () -> {
        int type = (int) (Math.random() * 5);
        switch (type) {
          case 0:
            client.submitCommand(HeapBuffer.allocate()
              .writeByte(1)
              .writeString(randomKey())
              .writeString(UUID.randomUUID().toString())
              .flip());
            break;
          case 1:
            client.submitQuery(HeapBuffer.allocate()
              .writeByte(2)
              .writeString(randomKey())
              .flip(), randomConsistency());
            break;
          case 2:
            client.submitCommand(HeapBuffer.allocate()
              .writeByte(3)
              .writeString(randomKey())
              .flip());
            break;
        }
      });
    }

    scheduleRestarts(this.clients.get(0).context());

    Thread.sleep(Duration.ofMinutes(15).toMillis());
  }

  /**
   * Schedules a random number of servers to be shutdown for a period of time and then restarted.
   */
  private void scheduleRestarts(ThreadContext context) {
    if (shutdownTimers.isEmpty() && restartTimers.isEmpty()) {
      int shutdownCount = randomNumber(servers.size() - 1);
      for (int i = 0; i < shutdownCount; i++) {
        scheduleRestart(i, context);
      }
    }
  }

  /**
   * Schedules the given server to be shutdown for a period of time and then restarted.
   */
  private void scheduleRestart(int serverIndex, ThreadContext context) {
    shutdownTimers.put(serverIndex, context.schedule(Duration.ofSeconds(randomNumber(120) + 10), () -> {
      shutdownTimers.remove(serverIndex);
      CopycatServer server = servers.get(serverIndex);
      System.out.println("Shutting down server: " + server.cluster().member().address());
      server.shutdown().whenComplete((result, error) -> {
        restartTimers.put(serverIndex, context.schedule(Duration.ofSeconds(randomNumber(120) + 10), () -> {
          restartTimers.remove(serverIndex);
          CopycatServer newServer = createServer(server.cluster().member());
          System.out.println("Starting up server: " + newServer.cluster().member().address());
          servers.set(serverIndex, newServer);
          newServer.bootstrap(members.stream().map(Member::serverAddress).collect(Collectors.toList())).whenComplete((result2, error2) -> {
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
      .withClientProtocol(new NettyTcpProtocol())
      .withProtocol(new NettyTcpRaftProtocol())
      .withStorage(Storage.builder()
        .withStorageLevel(StorageLevel.DISK)
        .withDirectory(new File(String.format("target/fuzz-logs/%d", member.address().hashCode())))
        .withMaxSegmentSize(randomNumber(1024 * 16) + (1024 * 8))
        .withMaxEntriesPerSegment(randomNumber(1000) + 100)
        .withCompactionThreads(randomNumber(4) + 1)
        .withCompactionThreshold(Math.random())
        .withEntryBufferSize(randomNumber(100) + 1)
        .withFlushOnCommit(randomNumber(2) == 1)
        .withMinorCompactionInterval(Duration.ofSeconds(randomNumber(30) + 15))
        .withMajorCompactionInterval(Duration.ofSeconds(randomNumber(60) + 60))
        .build())
      .withStateMachine(FuzzStateMachine::new);

    CopycatServer server = builder.build();
    servers.add(server);
    return server;
  }

  /**
   * Creates a Copycat client.
   */
  private CopycatClient createClient(RecoveryStrategy strategy) throws Exception {
    CopycatClient client = CopycatClient.builder()
      .withProtocol(new NettyTcpProtocol())
      .withConnectionStrategy(ConnectionStrategies.FIBONACCI_BACKOFF)
      .withRecoveryStrategy(strategy)
      .build();
    CountDownLatch latch = new CountDownLatch(1);
    client.connect(members.stream().map(Member::clientAddress).collect(Collectors.toList())).thenRun(latch::countDown);
    latch.await(30, TimeUnit.SECONDS);
    clients.add(client);
    return client;
  }

  /**
   * Fuzz test state machine.
   */
  public static class FuzzStateMachine extends StateMachine implements SessionListener, Snapshottable {
    private Map<String, Commit> map = new HashMap<>();

    @Override
    public void register(Session session) {

    }

    @Override
    public void unregister(Session session) {

    }

    @Override
    public void expire(Session session) {

    }

    @Override
    public void close(Session session) {

    }

    @Override
    public Buffer apply(Commit commit) {
      int type = commit.buffer().readByte();
      switch (type) {
        case 1:
          return put(commit);
        case 2:
          return get(commit);
        case 3:
          return remove(commit);
      }
      return null;
    }

    @Override
    public void snapshot(SnapshotWriter writer) {
      writer.writeLong(10);
    }

    @Override
    public void install(SnapshotReader reader) {
      reader.readLong();
    }

    protected Buffer put(Commit commit) {
      try {
        String key = commit.buffer().readString();
        Commit old = map.put(key, commit);
        if (old != null) {
          try {
            return HeapBuffer.wrap(old.buffer().rewind().readBytes());
          } finally {
            old.close();
          }
        }
        return null;
      } catch (Exception e) {
        commit.compact(Commit.CompactionMode.QUORUM);
        throw e;
      }
    }

    protected Buffer get(Commit commit) {
      try {
        String key = commit.buffer().readString();
        Commit value = map.get(key);
        return value != null ? HeapBuffer.wrap(value.buffer().rewind().readBytes()) : null;
      } finally {
        commit.close();
      }
    }

    protected Buffer remove(Commit commit) {
      try {
        String key = commit.buffer().readString();
        Commit removed = map.remove(key);
        if (removed != null) {
          try {
            return HeapBuffer.wrap(removed.buffer().rewind().readBytes());
          } finally {
            removed.close();
          }
        }
        return null;
      } finally {
        commit.compact(Commit.CompactionMode.TOMBSTONE);
      }
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