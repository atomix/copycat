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

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.serializer.ServiceLoaderTypeResolver;
import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.state.Member;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

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

  /**
   * Returns a new metastore.
   */
  protected MetaStore createMetaStore() {
    return Storage.builder()
      .withSerializer(new Serializer(new ServiceLoaderTypeResolver()))
      .withDirectory(new File(String.format("target/test-logs/%s", testId)))
      .build()
      .openMetaStore("test");
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

    Collection<Member> members = Arrays.asList(
      new Member(CopycatServer.Type.ACTIVE, new Address("localhost", 5000), new Address("localhost", 6000)),
      new Member(CopycatServer.Type.ACTIVE, new Address("localhost", 5001), new Address("localhost", 6001)),
      new Member(CopycatServer.Type.ACTIVE, new Address("localhost", 5002), new Address("localhost", 6002))
    );
    meta.storeConfiguration(new Configuration(1, members));

    Configuration configuration = meta.loadConfiguration();
    assertEquals(configuration.version(), 1);
    assertTrue(configuration.members().contains(new Member(CopycatServer.Type.ACTIVE, new Address("localhost", 5000), new Address("localhost", 6000))));
    assertTrue(configuration.members().contains(new Member(CopycatServer.Type.ACTIVE, new Address("localhost", 5001), new Address("localhost", 6001))));
    assertTrue(configuration.members().contains(new Member(CopycatServer.Type.ACTIVE, new Address("localhost", 5002), new Address("localhost", 6002))));
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
    meta.delete();
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

}
