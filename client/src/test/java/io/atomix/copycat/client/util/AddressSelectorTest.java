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
package io.atomix.copycat.client.util;

import io.atomix.catalyst.transport.Address;
import io.atomix.copycat.client.ServerSelectionStrategies;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;

import static org.testng.Assert.*;

/**
 * Address selector test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class AddressSelectorTest {

  /**
   * Tests iterating an address selector.
   */
  public void testIterate() throws Throwable {
    Collection<Address> servers = Arrays.asList(
      new Address("localhost", 5000),
      new Address("localhost", 5001),
      new Address("localhost", 5002)
    );

    AddressSelector selector = new AddressSelector(ServerSelectionStrategies.ANY);
    selector.reset(null, servers);
    assertNull(selector.leader());
    assertEquals(selector.servers(), servers);
    assertEquals(selector.state(), AddressSelector.State.RESET);
    assertTrue(selector.hasNext());
    assertNotNull(selector.next());
    assertEquals(selector.state(), AddressSelector.State.ITERATE);
    assertTrue(selector.hasNext());
    assertNotNull(selector.next());
    assertTrue(selector.hasNext());
    assertNotNull(selector.next());
    assertFalse(selector.hasNext());
    assertEquals(selector.state(), AddressSelector.State.COMPLETE);
  }

  /**
   * Tests resetting an address selector.
   */
  public void testReset() throws Throwable {
    Collection<Address> servers = Arrays.asList(
      new Address("localhost", 5000),
      new Address("localhost", 5001),
      new Address("localhost", 5002)
    );

    AddressSelector selector = new AddressSelector(ServerSelectionStrategies.ANY);
    selector.reset(null, servers);
    selector.next();
    selector.next();
    selector.next();
    assertFalse(selector.hasNext());
    selector.reset();
    assertTrue(selector.hasNext());
    assertEquals(selector.state(), AddressSelector.State.RESET);
    selector.next();
    assertEquals(selector.state(), AddressSelector.State.ITERATE);
  }

  /**
   * Tests updating the members in a selector.
   */
  public void testUpdate() throws Throwable {
    Collection<Address> servers = Arrays.asList(
      new Address("localhost", 5000),
      new Address("localhost", 5001),
      new Address("localhost", 5002)
    );

    AddressSelector selector = new AddressSelector(ServerSelectionStrategies.ANY);
    selector.reset(null, servers);
    assertNull(selector.leader());
    assertEquals(selector.servers(), servers);
    selector.next();
    assertEquals(selector.state(), AddressSelector.State.ITERATE);
    selector.reset(new Address("localhost", 5000), servers);
    assertEquals(selector.leader(), new Address("localhost", 5000));
    assertEquals(selector.servers(), servers);
    assertEquals(selector.state(), AddressSelector.State.RESET);
  }

}
