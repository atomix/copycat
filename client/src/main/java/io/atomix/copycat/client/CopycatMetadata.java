/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.atomix.copycat.client;

import io.atomix.copycat.metadata.CopycatClientMetadata;
import io.atomix.copycat.metadata.CopycatSessionMetadata;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Copycat metadata.
 */
public interface CopycatMetadata {

  /**
   * Returns a list of clients connected to the cluster.
   *
   * @return A completable future to be completed with a list of clients connected to the cluster.
   */
  CompletableFuture<Set<CopycatClientMetadata>> getClients();

  /**
   * Returns a list of open sessions.
   *
   * @return A completable future to be completed with a list of open sessions.
   */
  CompletableFuture<Set<CopycatSessionMetadata>> getSessions();

  /**
   * Returns a list of open sessions of the given type.
   *
   * @return A completable future to be completed with a list of open sessions of the given type.
   */
  CompletableFuture<Set<CopycatSessionMetadata>> getSessions(String type);

}
