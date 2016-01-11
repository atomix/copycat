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
package io.atomix.copycat.server.cluster;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Listener;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Copycat cluster info.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Cluster {

  /**
   * Returns the current cluster leader.
   *
   * @return The current cluster leader.
   */
  Member leader();

  /**
   * Returns the current cluster term.
   *
   * @return The current cluster term.
   */
  long term();

  /**
   * Registers a callback to be called when a leader is elected.
   *
   * @param callback The callback to be called when a new leader is elected.
   * @return The leader election listener.
   */
  Listener<Member> onLeaderElection(Consumer<Member> callback);

  /**
   * Returns the local cluster member.
   *
   * @return The local cluster member.
   */
  Member member();

  /**
   * Returns a member by ID.
   *
   * @param id The member ID.
   * @return The member or {@code null} if no member with the given {@code id} exists.
   */
  Member member(int id);

  /**
   * Returns a member by address.
   *
   * @param address The member server address.
   * @return The member or {@code null} if no member with the given {@link Address} exists.
   */
  Member member(Address address);

  /**
   * Returns a collection of all cluster members.
   *
   * @return A collection of all cluster members.
   */
  Collection<Member> members();

  /**
   * Joins the cluster.
   *
   * @return A completable future to be completed once the local server has joined the cluster.
   */
  CompletableFuture<Void> join();

  /**
   * Leaves the cluster.
   *
   * @return A completable future to be completed once the local server has left the cluster.
   */
  CompletableFuture<Void> leave();

  /**
   * Registers a callback to be called when a member joins the cluster.
   *
   * @param callback The callback to be called when a member joins the cluster.
   * @return The join listener.
   */
  Listener<Member> onJoin(Consumer<Member> callback);

  /**
   * Registers a callback to be called when a member leaves the cluster.
   *
   * @param callback The callback to be called when a member leaves the cluster.
   * @return The leave listener.
   */
  Listener<Member> onLeave(Consumer<Member> callback);

}
