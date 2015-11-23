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
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.transport.Address;

/**
 * Contains metadata and connection information related to a single member of the cluster.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
@SerializeWith(id=230)
public class Member implements CatalystSerializable {

  /**
   * Member type.
   */
  enum Type {
    ACTIVE,
    PASSIVE,
    RESERVE
  }

  private Type type;
  private Address serverAddress;
  private Address clientAddress;

  Member() {
  }

  public Member(Type type, Address serverAddress) {
    this(type, serverAddress, null);
  }

  public Member(Address serverAddress) {
    this(null, serverAddress, null);
  }

  public Member(Address serverAddress, Address clientAddress) {
    this(null, serverAddress, clientAddress);
  }

  public Member(Type type, Address serverAddress, Address clientAddress) {
    this.type = type;
    this.serverAddress = serverAddress;
    this.clientAddress = clientAddress;
  }

  /**
   * Returns the member ID.
   *
   * @return The member ID.
   */
  public int id() {
    return hashCode();
  }

  /**
   * Returns the member type.
   *
   * @return The member type.
   */
  public Type type() {
    return type;
  }

  /**
   * Returns a boolean indicating whether the member is active.
   *
   * @return Whether the member is active.
   */
  boolean isActive() {
    return type == Type.ACTIVE;
  }

  /**
   * Returns a boolean indicating whether the member is passive.
   *
   * @return Whether the member is passive.
   */
  boolean isPassive() {
    return type == Type.PASSIVE;
  }

  /**
   * Returns a boolean indicating whether the member is reserve.
   *
   * @return Whether the member is reserve.
   */
  boolean isReserve() {
    return type == Type.RESERVE;
  }

  /**
   * Returns the server address.
   *
   * @return The server address.
   */
  public Address serverAddress() {
    return serverAddress;
  }

  /**
   * Returns the client address.
   *
   * @return The client address.
   */
  public Address clientAddress() {
    return clientAddress;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    serializer.writeObject(type, buffer);
    serializer.writeObject(serverAddress, buffer);
    serializer.writeObject(clientAddress, buffer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    type = serializer.readObject(buffer);
    serverAddress = serializer.readObject(buffer);
    clientAddress = serializer.readObject(buffer);
  }

  @Override
  public int hashCode() {
    return serverAddress.hashCode();
  }

  @Override
  public boolean equals(Object object) {
    return object instanceof Member && ((Member) object).serverAddress().equals(serverAddress);
  }

  @Override
  public String toString() {
    return String.format("%s[type=%s, server=%s, client=%s]", getClass().getSimpleName(), type, serverAddress, clientAddress);
  }

}
