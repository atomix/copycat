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

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.concurrent.Listener;
import io.atomix.catalyst.concurrent.Listeners;
import io.atomix.catalyst.concurrent.Scheduled;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.Response;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.ReconfigureRequest;
import io.atomix.copycat.server.storage.system.Configuration;

import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * Cluster member.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public final class ServerMember implements Member, CatalystSerializable, AutoCloseable {
  private Member.Type type;
  private Status status = Status.AVAILABLE;
  private Instant updated;
  private Address serverAddress;
  private Address clientAddress;
  private transient Scheduled configureTimeout;
  private transient ClusterState cluster;
  private transient Listeners<Type> typeChangeListeners;
  private transient Listeners<Status> statusChangeListeners;

  ServerMember() {
  }

  public ServerMember(Member.Type type, Address serverAddress, Address clientAddress, Instant updated) {
    this.type = Assert.notNull(type, "type");
    this.serverAddress = Assert.notNull(serverAddress, "serverAddress");
    this.clientAddress = clientAddress;
    this.updated = Assert.notNull(updated, "updated");
  }

  /**
   * Sets the member's parent cluster.
   */
  ServerMember setCluster(ClusterState cluster) {
    this.cluster = cluster;
    return this;
  }

  @Override
  public int id() {
    return hashCode();
  }

  @Override
  public Member.Type type() {
    return type;
  }

  @Override
  public Status status() {
    return status;
  }

  @Override
  public Instant updated() {
    return updated;
  }

  @Override
  public Address address() {
    return serverAddress();
  }

  /**
   * Returns the server address.
   *
   * @return The server address.
   */
  @Override
  public Address serverAddress() {
    return serverAddress;
  }

  /**
   * Returns the client address.
   *
   * @return The client address.
   */
  @Override
  public Address clientAddress() {
    return clientAddress;
  }

  @Override
  public Listener<Type> onTypeChange(Consumer<Type> callback) {
    if (typeChangeListeners == null)
      typeChangeListeners = new Listeners<>();
    return typeChangeListeners.add(callback);
  }

  @Override
  public Listener<Status> onStatusChange(Consumer<Status> callback) {
    if (statusChangeListeners == null)
      statusChangeListeners = new Listeners<>();
    return statusChangeListeners.add(callback);
  }

  @Override
  public CompletableFuture<Void> promote() {
    return configure(Type.values()[type.ordinal() + 1]);
  }

  @Override
  public CompletableFuture<Void> promote(Type type) {
    return configure(type);
  }

  @Override
  public CompletableFuture<Void> demote() {
    return configure(Type.values()[type.ordinal() - 1]);
  }

  @Override
  public CompletableFuture<Void> demote(Type type) {
    return configure(type);
  }

  @Override
  public CompletableFuture<Void> remove() {
    return configure(Type.INACTIVE);
  }

  /**
   * Updates the member type.
   *
   * @param type The member type.
   * @return The member.
   */
  ServerMember update(Member.Type type, Instant time) {
    if (this.type != type) {
      this.type = Assert.notNull(type, "type");
      if (time.isAfter(updated)) {
        this.updated = Assert.notNull(time, "time");
      }
      if (typeChangeListeners != null) {
        typeChangeListeners.accept(type);
      }
    }
    return this;
  }

  /**
   * Updates the member status.
   *
   * @param status The member status.
   * @return The member.
   */
  ServerMember update(Status status, Instant time) {
    if (this.status != status) {
      this.status = Assert.notNull(status, "status");
      if (time.isAfter(updated)) {
        this.updated = Assert.notNull(time, "time");
      }
      if (statusChangeListeners != null) {
        statusChangeListeners.accept(status);
      }
    }
    return this;
  }

  /**
   * Updates the member client address.
   *
   * @param clientAddress The member client address.
   * @return The member.
   */
  ServerMember update(Address clientAddress, Instant time) {
    if (clientAddress != null) {
      this.clientAddress = clientAddress;
      if (time.isAfter(updated)) {
        this.updated = Assert.notNull(time, "time");
      }
    }
    return this;
  }

  /**
   * Demotes the server to the given type.
   */
  CompletableFuture<Void> configure(Member.Type type) {
    CompletableFuture<Void> future = new CompletableFuture<>();
    cluster.getContext().getThreadContext().executor().execute(() -> configure(type, future));
    return future;
  }

  /**
   * Recursively reconfigures the cluster.
   */
  private void configure(Member.Type type, CompletableFuture<Void> future) {
    // Set a timer to retry the attempt to leave the cluster.
    configureTimeout = cluster.getContext().getThreadContext().schedule(cluster.getContext().getElectionTimeout(), () -> {
      configure(type, future);
    });

    // Attempt to leave the cluster by submitting a LeaveRequest directly to the server state.
    // Non-leader states should forward the request to the leader if there is one. Leader states
    // will log, replicate, and commit the reconfiguration.
    cluster.getContext().getServerState().reconfigure(ReconfigureRequest.builder()
      .withIndex(cluster.getConfiguration().index())
      .withTerm(cluster.getConfiguration().term())
      .withMember(new ServerMember(type, serverAddress(), clientAddress(), updated))
      .build()).whenComplete((response, error) -> {
      if (error == null) {
        if (response.status() == Response.Status.OK) {
          cancelConfigureTimer();
          cluster.configure(new Configuration(response.index(), response.term(), response.timestamp(), response.members()));
          future.complete(null);
        } else if (response.error() == null || response.error() == CopycatError.Type.NO_LEADER_ERROR) {
          cancelConfigureTimer();
          configureTimeout = cluster.getContext().getThreadContext().schedule(cluster.getContext().getElectionTimeout().multipliedBy(2), () -> configure(type, future));
        } else {
          cancelConfigureTimer();
          future.completeExceptionally(response.error().createException());
        }
      }
    });
  }

  /**
   * Cancels the configure timeout.
   */
  private void cancelConfigureTimer() {
    if (configureTimeout != null) {
      configureTimeout.cancel();
      configureTimeout = null;
    }
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeByte(type.ordinal());
    buffer.writeByte(status.ordinal());
    buffer.writeLong(updated.toEpochMilli());
    serializer.writeObject(serverAddress, buffer);
    serializer.writeObject(clientAddress, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    type = Member.Type.values()[buffer.readByte()];
    status = Status.values()[buffer.readByte()];
    updated = Instant.ofEpochMilli(buffer.readLong());
    serverAddress = serializer.readObject(buffer);
    clientAddress = serializer.readObject(buffer);
  }

  @Override
  public void close() {
    cancelConfigureTimer();
  }

  @Override
  public int hashCode() {
    return serverAddress.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof ServerMember && ((ServerMember) object).serverAddress().equals(serverAddress);
  }

  @Override
  public String toString() {
    return String.format("%s[type=%s, status=%s, serverAddress=%s, clientAddress=%s]", getClass().getSimpleName(), type, status, serverAddress, clientAddress);
  }

}
