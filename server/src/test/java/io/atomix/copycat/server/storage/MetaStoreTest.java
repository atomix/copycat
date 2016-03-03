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
package io.atomix.copycat.server.storage;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Listener;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.storage.system.Configuration;
import io.atomix.copycat.server.storage.system.MetaStore;
import io.atomix.copycat.server.storage.util.StorageSerialization;
import io.atomix.copycat.server.util.ServerSerialization;
import io.atomix.copycat.util.ProtocolSerialization;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Metastore test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class MetaStoreTest {
  private String testId;
  private Storage storage;

  /**
   * Returns a new metastore.
   */
  protected MetaStore createMetaStore() {
    storage = Storage.builder()
      .withDirectory(new File(String.format("target/test-logs/%s", testId)))
      .build();
    return new MetaStore("test", storage, new Serializer().resolve(new ProtocolSerialization(), new ServerSerialization(), new StorageSerialization()).register(TestMember.class));
  }

  /**
   * Tests storing and loading data from the metastore.
   */
  @SuppressWarnings("unchecked")
  public void testMetaStore() {
    MetaStore meta = createMetaStore();

    assertEquals(meta.loadTerm(), 0);
    assertEquals(meta.loadVote(), 0);

    meta.storeTerm(1);
    meta.storeVote(2);

    assertEquals(meta.loadTerm(), 1);
    assertEquals(meta.loadVote(), 2);

    Collection<Member> members = new ArrayList<>(Arrays.asList(
      new TestMember(Member.Type.ACTIVE, new Address("localhost", 5000), new Address("localhost", 6000)),
      new TestMember(Member.Type.ACTIVE, new Address("localhost", 5001), new Address("localhost", 6001)),
      new TestMember(Member.Type.ACTIVE, new Address("localhost", 5002), new Address("localhost", 6002))
    ));
    meta.storeConfiguration(new Configuration(1, 1, System.currentTimeMillis(), members));

    Configuration configuration = meta.loadConfiguration();
    assertEquals(configuration.index(), 1);
    assertTrue(configuration.members().contains(new TestMember(Member.Type.ACTIVE, new Address("localhost", 5000), new Address("localhost", 6000))));
    assertTrue(configuration.members().contains(new TestMember(Member.Type.ACTIVE, new Address("localhost", 5001), new Address("localhost", 6001))));
    assertTrue(configuration.members().contains(new TestMember(Member.Type.ACTIVE, new Address("localhost", 5002), new Address("localhost", 6002))));
  }

  /**
   * Tests deleting a metastore.
   */
  public void testDeleteMetaStore() throws Throwable {
    MetaStore meta = createMetaStore();
    assertEquals(meta.loadTerm(), 0);
    assertEquals(meta.loadVote(), 0);
    meta.storeTerm(1);
    meta.storeVote(2);
    assertEquals(meta.loadTerm(), 1);
    assertEquals(meta.loadVote(), 2);
    meta = createMetaStore();
    assertEquals(meta.loadTerm(), 1);
    assertEquals(meta.loadVote(), 2);
    storage.deleteMetaStore("test");
    meta = createMetaStore();
    assertEquals(meta.loadTerm(), 0);
    assertEquals(meta.loadVote(), 0);
  }

  @BeforeMethod
  @AfterMethod
  protected void cleanupStorage() throws IOException {
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
    testId = UUID.randomUUID().toString();
  }

  /**
   * Test member.
   */
  public static class TestMember implements Member, CatalystSerializable {
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

    @Override
    public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
      buffer.writeByte(type.ordinal());
      serializer.writeObject(serverAddress, buffer);
      serializer.writeObject(clientAddress, buffer);
    }

    @Override
    public void readObject(BufferInput<?> buffer, Serializer serializer) {
      type = Type.values()[buffer.readByte()];
      serverAddress = serializer.readObject(buffer);
      clientAddress = serializer.readObject(buffer);
    }

    @Override
    public boolean equals(Object object) {
      if (object instanceof Member) {
        Member member = (Member) object;
        return member.type() == type() && member.serverAddress().equals(serverAddress());
      }
      return false;
    }
  }

}
