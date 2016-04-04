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
import io.atomix.copycat.client.ServerSelectionStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Client address selector.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class AddressSelector implements Iterator<Address> {

  /**
   * Address selector state.
   */
  public enum State {

    /**
     * Indicates that the selector has been reset.
     */
    RESET,

    /**
     * Indicates that the selector is being iterated.
     */
    ITERATE,

    /**
     * Indicates that selector iteration is complete.
     */
    COMPLETE

  }

  private Address leader;
  private Collection<Address> servers = new ArrayList<>();
  private final ServerSelectionStrategy strategy;
  private Collection<Address> selections = new ArrayList<>();
  private Iterator<Address> selectionsIterator;

  public AddressSelector(ServerSelectionStrategy strategy) {
    this.strategy = Assert.notNull(strategy, "strategy");
  }

  /**
   * Returns the address selector state.
   *
   * @return The address selector state.
   */
  public State state() {
    if (selectionsIterator == null) {
      return State.RESET;
    } else if (hasNext()) {
      return State.ITERATE;
    } else {
      return State.COMPLETE;
    }
  }

  /**
   * Returns the current selector leader.
   *
   * @return The current selector leader.
   */
  public Address leader() {
    return leader;
  }

  /**
   * Returns the current set of servers.
   *
   * @return The current set of servers.
   */
  public Collection<Address> servers() {
    return servers;
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
    if (changed(leader, servers)) {
      this.leader = leader;
      this.servers = servers;
      this.selections = strategy.selectConnections(leader, new ArrayList<>(servers));
      this.selectionsIterator = null;
    }
    return this;
  }

  /**
   * Returns a boolean value indicating whether the selector state would be changed by the given members.
   */
  private boolean changed(Address leader, Collection<Address> servers) {
    Assert.notNull(servers, "servers");
    Assert.argNot(servers.isEmpty(), "servers list cannot be empty");
    if (this.leader != null && leader == null) {
      return true;
    } else if (this.leader == null && leader != null) {
      Assert.arg(servers.contains(leader), "leader must be present in servers list");
      return true;
    } else if (this.leader != null && !this.leader.equals(leader)) {
      Assert.arg(servers.contains(leader), "leader must be present in servers list");
      return true;
    } else if (!matches(this.servers, servers)) {
      return true;
    }
    return false;
  }

  /**
   * Returns a boolean value indicating whether the servers in the first list match the servers in the second list.
   */
  private boolean matches(Collection<Address> left, Collection<Address> right) {
    if (left.size() != right.size())
      return false;

    for (Address address : left) {
      if (!right.contains(address)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean hasNext() {
    return selectionsIterator == null ? !selections.isEmpty() : selectionsIterator.hasNext();
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
