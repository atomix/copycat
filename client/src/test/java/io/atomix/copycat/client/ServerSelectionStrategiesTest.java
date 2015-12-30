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
package io.atomix.copycat.client;

import io.atomix.catalyst.transport.Address;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Server selection strategies test.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@Test
public class ServerSelectionStrategiesTest {
  private final List<Address> servers = Arrays.asList(
    new Address("localhost", 5000),
    new Address("localhost", 5001),
    new Address("localhost", 5002)
  );

  /**
   * Tests the ANY server selection strategy.
   */
  public void testAnySelectionStrategy() throws Throwable {
    List<Address> results = (List<Address>) ServerSelectionStrategies.ANY.selectConnections(null, servers);
    assertTrue(listsEqual(results, servers));
  }

  /**
   * Tests the LEADER server selection strategy.
   */
  public void testLeaderSelectionStrategy() throws Throwable {
    List<Address> results = (List<Address>) ServerSelectionStrategies.LEADER.selectConnections(new Address("localhost", 5000), servers);
    assertEquals(results.size(), 1);
    assertEquals(results.get(0), new Address("localhost", 5000));
  }

  /**
   * Tests the LEADER server selection strategy.
   */
  public void testNoLeaderSelectionStrategy() throws Throwable {
    List<Address> results = (List<Address>) ServerSelectionStrategies.LEADER.selectConnections(null, servers);
    assertTrue(listsEqual(results, servers));
  }

  /**
   * Tests the FOLLOWERS server selection strategy.
   */
  public void testFollowersNoLeaderSelectionStrategy() throws Throwable {
    List<Address> results = (List<Address>) ServerSelectionStrategies.FOLLOWERS.selectConnections(null, servers);
    assertTrue(listsEqual(results, servers));
  }

  /**
   * Tests the FOLLOWERS server selection strategy.
   */
  public void testFollowersSelectionStrategy() throws Throwable {
    List<Address> results = (List<Address>) ServerSelectionStrategies.FOLLOWERS.selectConnections(new Address("localhost", 5000), servers);
    assertEquals(results.size(), servers.size() - 1);
    assertFalse(results.contains(new Address("localhost", 5000)));
  }

  /**
   * Returns a boolean value indicating whether the servers in the first list match the servers in the second list.
   */
  private boolean listsEqual(Collection<Address> left, Collection<Address> right) {
    if (left.size() != right.size())
      return false;

    for (Address address : left) {
      if (!right.contains(address)) {
        return false;
      }
    }
    return true;
  }

}
