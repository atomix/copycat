/*
 * Copyright 2016 the original author or authors.
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

import io.atomix.copycat.client.ConnectionStrategies;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.server.Commit;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.StateMachine;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;
import io.atomix.copycat.util.buffer.Buffer;
import io.atomix.copycat.util.buffer.HeapBuffer;
import io.atomix.copycat.util.concurrent.Listener;
import net.jodah.concurrentunit.ConcurrentTestCase;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Performance test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class PerformanceTest extends ConcurrentTestCase {
  protected volatile int port;
  protected volatile List<Member> members;
  protected volatile List<CopycatClient> clients = new ArrayList<>();
  protected volatile List<CopycatServer> servers = new ArrayList<>();

  /**
   * Tests performance.
   */
  /*public void testPerformance() throws Throwable{
    createServers(3);

    CopycatClient client = createClient();

    AtomicInteger counter = new AtomicInteger();
    AtomicLong timer = new AtomicLong();
    client.context().schedule(Duration.ofSeconds(1), Duration.ofSeconds(1), () -> {
      long count = counter.get();
      long time = System.currentTimeMillis();
      long previousTime = timer.get();
      if (previousTime > 0) {
        System.out.println(String.format("Completed %d operations in %d milliseconds", count, time - previousTime));
      }
      counter.set(0);
      timer.set(time);
    });

    for (int i = 0; i < 100; i++) {
      recursiveCommand(client, counter);
    }

    for (;;);
  }*/

  private static void recursiveCommand(CopycatClient client, AtomicInteger counter) {
    client.submitCommand(HeapBuffer.allocate().writeString(UUID.randomUUID().toString()).flip()).thenRun(() -> {
      counter.incrementAndGet();
      recursiveCommand(client, counter);
    });
  }

  private static void recursiveQuery(CopycatClient client, AtomicInteger counter) {
    client.submitQuery(HeapBuffer.allocate().writeString(UUID.randomUUID().toString()).flip()).thenRun(() -> {
      counter.incrementAndGet();
      recursiveQuery(client, counter);
    });
  }

  private static void recursiveSwitchQuery(CopycatClient client, AtomicInteger counter) {
    client.submitQuery(HeapBuffer.allocate().writeString(UUID.randomUUID().toString()).flip()).thenRun(() -> {
      counter.incrementAndGet();
      recursiveSwitchCommand(client, counter);
    });
  }

  private static void recursiveSwitchCommand(CopycatClient client, AtomicInteger counter) {
    client.submitCommand(HeapBuffer.allocate().writeString(UUID.randomUUID().toString()).flip()).thenRun(() -> {
      counter.incrementAndGet();
      recursiveSwitchQuery(client, counter);
    });
  }

  private Member nextMember() {
    return nextMember(Member.Type.INACTIVE);
  }

  private Member nextMember(Member.Type type) {
    return new TestMember(type, new Address("localhost", ++port), new Address("localhost", port + 1000));
  }

  private List<CopycatServer> createServers(int nodes) throws Throwable {
    List<CopycatServer> servers = new ArrayList<>();

    for (int i = 0; i < nodes; i++) {
      members.add(nextMember());
    }

    for (int i = 0; i < nodes; i++) {
      CopycatServer server = createServer(members.get(i));
      server.bootstrap(members.stream().map(Member::serverAddress).collect(Collectors.toList())).thenRun(this::resume);
      servers.add(server);
    }

    await(30000 * nodes, nodes);

    return servers;
  }

  private CopycatServer createServer(Member member) {
    CopycatServer.Builder builder = CopycatServer.builder(member.clientAddress(), member.serverAddress())
      .withStorage(Storage.builder()
        .withDirectory(new File(String.format("target/test-logs/%s", member.id())))
        .withStorageLevel(StorageLevel.MAPPED)
        .withMaxSegmentSize(1024 * 1024 * 32)
        .withCompactionThreads(1)
        .build())
      .withStateMachine(TestStateMachine::new);

    if (member.type() != Member.Type.INACTIVE) {
      builder.withType(member.type());
    }

    CopycatServer server = builder.build();
    servers.add(server);
    return server;
  }

  private CopycatClient createClient() throws Throwable {
    CopycatClient client = CopycatClient.builder(members.stream().map(Member::clientAddress).collect(Collectors.toList()))
      .withConnectionStrategy(ConnectionStrategies.FIBONACCI_BACKOFF)
      .build();
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
          s.shutdown();
        }
      } catch (Exception e) {
      }
    });

    members = new ArrayList<>();
    port = 5000;
    clients = new ArrayList<>();
    servers = new ArrayList<>();

    Path directory = Paths.get("target/test-logs/");
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

  public static class TestStateMachine extends StateMachine {
    private Commit last;

    @Override
    public Buffer apply(Commit commit) {
      return (Buffer) commit.buffer();
    }
  }

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
