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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.protocol.Address;
import io.atomix.copycat.util.Assert;

/**
 * Stores a connection between a client and server.
 * <p>
 * The {@code ConnectEntry} is used to represent the establishment of a connection between a
 * specific {@link #client() client} and {@link #address() server}. Storing and replicating
 * connections allows servers to share a consistent view of the clients connected to each server.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ConnectEntry extends TimestampedEntry<ConnectEntry> {
  private final String client;
  private final Address address;

  public ConnectEntry(long timestamp, String client, Address address) {
    super(timestamp);
    this.client = Assert.notNull(client, "client");
    this.address = Assert.notNull(address, "address");
  }

  @Override
  public Type<ConnectEntry> type() {
    return Type.CONNECT;
  }

  /**
   * Returns the entry client ID.
   *
   * @return The entry client ID.
   */
  public String client() {
    return client;
  }

  /**
   * Returns the connection address.
   *
   * @return The connection address.
   */
  public Address address() {
    return address;
  }

  @Override
  public String toString() {
    return String.format("%s[client=%s, address=%s, timestamp=%d]", getClass().getSimpleName(), client(), address(), timestamp());
  }

  /**
   * Connect entry serializer.
   */
  public static class Serializer extends TimestampedEntry.Serializer<ConnectEntry> {
    @Override
    public void write(Kryo kryo, Output output, ConnectEntry entry) {
      output.writeLong(entry.timestamp);
      output.writeString(entry.client);
      kryo.writeObject(output, entry.address);
    }

    @Override
    public ConnectEntry read(Kryo kryo, Input input, Class<ConnectEntry> type) {
      return new ConnectEntry(input.readLong(), input.readString(), kryo.readObject(input, Address.class));
    }
  }
}
