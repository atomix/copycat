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
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.SelectionStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Client address selector.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class AddressSelector implements Iterator<Address> {
  private Address leader;
  private Collection<Address> servers;
  private final SelectionStrategy strategy;
  private Iterable<Address> selections;
  private Iterator<Address> selectionsIterator;

  public AddressSelector(Collection<Address> servers, SelectionStrategy strategy) {
    this.servers = Assert.argNot(servers, Assert.notNull(servers, "servers").isEmpty(), "servers list cannot be empty");
    this.strategy = Assert.notNull(strategy, "strategy");
  }

  /**
   * Resets the addresses.
   *
   * @return The address selector.
   */
  public AddressSelector reset() {
    if (selectionsIterator != null) {
      this.selections = strategy.selectConnections(leader, new ArrayList<>(servers));
      this.selectionsIterator = null;
    }
    return this;
  }

  /**
   * Resets the connection addresses.
   *
   * @param servers The collection of server addresses.
   * @return The address selector.
   */
  public AddressSelector reset(Address leader, Collection<Address> servers) {
    Assert.notNull(servers, "servers");
    Assert.argNot(servers.isEmpty(), "servers list cannot be empty");
    if (leader != null)
      Assert.arg(servers.contains(leader), "leader must be present in servers list");
    this.leader = leader;
    this.servers = servers;
    this.selections = strategy.selectConnections(leader, new ArrayList<>(servers));
    this.selectionsIterator = null;
    return this;
  }

  @Override
  public boolean hasNext() {
    return selectionsIterator == null || selectionsIterator.hasNext();
  }

  @Override
  public Address next() {
    if (selectionsIterator == null)
      selectionsIterator = selections.iterator();
    return selectionsIterator.next();
  }

  @Override
  public String toString() {
    return String.format("%s[strategy=%s]", getClass().getSimpleName(), strategy);
  }

}
