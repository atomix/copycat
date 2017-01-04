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
package io.atomix.copycat.server.state;

import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.storage.util.StorageSerialization;
import io.atomix.copycat.server.util.ServerSerialization;
import io.atomix.copycat.util.ProtocolSerialization;
import org.testng.annotations.Test;

import java.time.Instant;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Member test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class MemberTest {

  /**
   * Tests member getters.
   */
  public void testMemberGetters() {
    Instant instant = Instant.now();
    ServerMember member = new ServerMember(Member.Type.ACTIVE, new Address("localhost", 5000), new Address("localhost", 6000), instant);
    assertEquals(member.type(), Member.Type.ACTIVE);
    assertEquals(member.status(), ServerMember.Status.AVAILABLE);
    assertEquals(member.serverAddress(), new Address("localhost", 5000));
    assertEquals(member.clientAddress(), new Address("localhost", 6000));
    assertEquals(member.updated(), instant);
  }

  /**
   * Tests serializing and deserializing a member.
   */
  public void testSerializeDeserialize() {
    Instant instant = Instant.now();
    ServerMember member = new ServerMember(Member.Type.ACTIVE, new Address("localhost", 5000), null, instant);
    Serializer serializer = new Serializer().resolve(new ProtocolSerialization(), new ServerSerialization(), new StorageSerialization());
    ServerMember result = serializer.readObject(serializer.writeObject(member).flip());
    assertEquals(result.type(), member.type());
    assertEquals(result.updated(), member.updated());
  }

  /**
   * Tests updating a member.
   */
  public void testMemberUpdate() {
    Instant instant = Instant.now();
    ServerMember member = new ServerMember(Member.Type.ACTIVE, new Address("localhost", 5000), null, instant);
    instant = Instant.now();
    member.update(Member.Type.INACTIVE, instant);
    assertEquals(member.type(), Member.Type.INACTIVE);
    assertEquals(member.updated(), instant);
    instant = Instant.now();
    member.update(ServerMember.Status.UNAVAILABLE, instant);
    assertEquals(member.status(), ServerMember.Status.UNAVAILABLE);
    assertNull(member.clientAddress());
    assertEquals(member.updated(), instant);
    instant = Instant.now();
    member.update(new Address("localhost", 6000), instant);
    assertEquals(member.clientAddress(), new Address("localhost", 6000));
    assertEquals(member.updated(), instant);
  }

}
