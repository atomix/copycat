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
package io.atomix.copycat.protocol;

import io.atomix.catalyst.util.Assert;

/**
 * Network address.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class Address {
  private String host;
  private int port;

  public Address() {
  }

  public Address(String address) {
    Assert.notNull(address, "address");
    String[] components = address.split(":");
    Assert.arg(components.length == 2, "%s must contain address:port", address);

    this.host = components[0];
    try {
      this.port = Integer.parseInt(components[1]);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(components[1] + " is not a number");
    }
  }

  public Address(Address address) {
    this(address.host, address.port);
  }

  public Address(String host, int port) {
    this.host = host;
    this.port = port;
  }

  /**
   * Returns the address host.
   *
   * @return The address host.
   */
  public String host() {
    return host;
  }

  /**
   * Returns the address port.
   *
   * @return The address port.
   */
  public int port() {
    return port;
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof Address) {
      Address address = (Address) object;
      return address.host.equals(host) && address.port == port;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int hashCode = 23;
    hashCode = 37 * hashCode + host.hashCode();
    hashCode = 37 * hashCode + port;
    return hashCode;
  }

  @Override
  public String toString() {
    return String.format("%s:%d", host, port);
  }

}
