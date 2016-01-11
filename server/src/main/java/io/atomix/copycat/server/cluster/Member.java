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

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Cluster member.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public interface Member {

  /**
   * Member type.
   */
  enum Type {

    /**
     * Inactive member type.
     */
    INACTIVE,

    /**
     * Reserve member type.
     */
    RESERVE,

    /**
     * Passive member type.
     */
    PASSIVE,

    /**
     * Promotable member type.
     */
    PROMOTABLE,

    /**
     * Active member type.
     */
    ACTIVE,

  }

  /**
   * Member status.
   */
  enum Status {

    /**
     * Available member status.
     */
    AVAILABLE,

    /**
     * Unavailable member status.
     */
    UNAVAILABLE,

  }

  /**
   * Returns the member ID.
   *
   * @return The member ID.
   */
  int id();

  /**
   * Returns the member server address.
   *
   * @return The member server address.
   */
  Address address();

  /**
   * Returns the member client address.
   *
   * @return The member client address.
   */
  Address clientAddress();

  /**
   * Returns the member server address.
   *
   * @return The member server address.
   */
  Address serverAddress();

  /**
   * Returns the member type.
   *
   * @return The member type.
   */
  Type type();

  /**
   * Registers a callback to be called when the member's type changes.
   *
   * @param callback The callback to be called when the member's type changes.
   * @return The member type change listener.
   */
  Listener<Type> onTypeChange(Consumer<Type> callback);

  /**
   * Returns the member status.
   *
   * @return The member status.
   */
  Status status();

  /**
   * Registers a callback to be called when the member's status changes.
   *
   * @param callback The callback to be called when the member's status changes.
   * @return The member status change listener.
   */
  Listener<Status> onStatusChange(Consumer<Status> callback);

  /**
   * Promotes the member to the next highest type.
   *
   * @return A completable future to be completed once the member has been promoted.
   */
  CompletableFuture<Void> promote();

  /**
   * Promotes the member to the given type.
   *
   * @param type The type to which to promote the member.
   * @return A completable future to be completed once the member has been promoted.
   */
  CompletableFuture<Void> promote(Member.Type type);

  /**
   * Demotes the member to the next lowest type.
   *
   * @return A completable future to be completed once the member has been demoted.
   */
  CompletableFuture<Void> demote();

  /**
   * Demotes the member to the given type.
   *
   * @param type The type to which to demote the member.
   * @return A completable future to be completed once the member has been demoted.
   */
  CompletableFuture<Void> demote(Member.Type type);

  /**
   * Removes the member from the configuration.
   *
   * @return A completable future to be completed once the member has been removed from the configuration.
   */
  CompletableFuture<Void> remove();

}
