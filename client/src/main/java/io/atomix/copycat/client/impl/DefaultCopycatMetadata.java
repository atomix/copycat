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
package io.atomix.copycat.client.impl;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.CommunicationStrategies;
import io.atomix.copycat.metadata.CopycatClientMetadata;
import io.atomix.copycat.client.CopycatMetadata;
import io.atomix.copycat.metadata.CopycatSessionMetadata;
import io.atomix.copycat.client.session.impl.CopycatClientConnection;
import io.atomix.copycat.client.util.AddressSelectorManager;
import io.atomix.copycat.client.util.ClientConnectionManager;
import io.atomix.copycat.protocol.MetadataRequest;
import io.atomix.copycat.protocol.MetadataResponse;
import io.atomix.copycat.protocol.Response;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Default Copycat metadata.
 */
public class DefaultCopycatMetadata implements CopycatMetadata {
  private final AddressSelectorManager selectorManager;
  private final CopycatClientConnection connection;

  public DefaultCopycatMetadata(ClientConnectionManager connectionManager, AddressSelectorManager selectorManager) {
    this.selectorManager = Assert.notNull(selectorManager, "selectorManager");
    this.connection = new CopycatClientConnection(connectionManager, selectorManager.createSelector(CommunicationStrategies.LEADER));
  }

  @Override
  public Address leader() {
    return selectorManager.leader();
  }

  @Override
  public Collection<Address> servers() {
    return selectorManager.servers();
  }

  /**
   * Requests metadata from the cluster.
   *
   * @return A completable future to be completed with cluster metadata.
   */
  private CompletableFuture<MetadataResponse> getMetadata() {
    CompletableFuture<MetadataResponse> future = new CompletableFuture<>();
    connection.<MetadataRequest, MetadataResponse>sendAndReceive(MetadataRequest.NAME, MetadataRequest.builder().build()).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          future.complete(response);
        } else {
          future.completeExceptionally(response.error().createException());
        }
      } else {
        future.completeExceptionally(error);
      }
    });
    return future;
  }

  @Override
  public CompletableFuture<Set<CopycatClientMetadata>> getClients() {
    return getMetadata().thenApply(MetadataResponse::clients);
  }

  @Override
  public CompletableFuture<Set<CopycatSessionMetadata>> getSessions() {
    return getMetadata().thenApply(MetadataResponse::sessions);
  }

  @Override
  public CompletableFuture<Set<CopycatSessionMetadata>> getSessions(String type) {
    return getMetadata().thenApply(response -> response.sessions().stream().filter(s -> s.type().equals(type)).collect(Collectors.toSet()));
  }

}
