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
 * limitations under the License.
 */
package io.atomix.copycat.server.storage.entry;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.ReferenceManager;

import java.util.UUID;

/**
 * Stores a connection between a client and server.
 * <p>
 * The {@code ConnectEntry} is used to represent the establishment of a connection between a
 * specific {@link #getClient() client} and {@link #getAddress() server}. Storing and replicating
 * connections allows servers to share a consistent view of the clients connected to each server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ConnectEntry extends TimestampedEntry<ConnectEntry> {
  private UUID client;
  private Address address;

  public ConnectEntry() {
  }

  public ConnectEntry(ReferenceManager<Entry<?>> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the entry client ID.
   *
   * @return The entry client ID.
   */
  public UUID getClient() {
    return client;
  }

  /**
   * Sets the entry client ID.
   *
   * @param client The entry client ID.
   * @return The register entry.
   * @throws NullPointerException if {@code client} is null
   */
  public ConnectEntry setClient(UUID client) {
    this.client = Assert.notNull(client, "client");
    return this;
  }

  /**
   * Returns the connection address.
   *
   * @return The connection address.
   */
  public Address getAddress() {
    return address;
  }

  /**
   * Sets the connection address.
   *
   * @param address The connection address.
   * @return The connect entry.
   * @throws NullPointerException if {@code address} is {@code null}
   */
  public ConnectEntry setAddress(Address address) {
    this.address = Assert.notNull(address, "address");
    return this;
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeString(client.toString());
    serializer.writeObject(address, buffer);
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    client = UUID.fromString(buffer.readString());
    address = serializer.readObject(buffer);
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, client=%s, address=%s, timestamp=%d]", getClass().getSimpleName(), getIndex(), getTerm(), getClient(), getAddress(), getTimestamp());
  }

}
