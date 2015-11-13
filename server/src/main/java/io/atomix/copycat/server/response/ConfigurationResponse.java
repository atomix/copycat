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
package io.atomix.copycat.server.response;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.error.RaftError;
import io.atomix.copycat.client.response.AbstractResponse;
import io.atomix.copycat.server.state.Member;

import java.util.Collection;
import java.util.Objects;

/**
 * Protocol configuration response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class ConfigurationResponse<T extends ConfigurationResponse<T>> extends AbstractResponse<T> {
  protected long version;
  protected Collection<Member> activeMembers;
  protected Collection<Member> passiveMembers;
  protected Collection<Member> reserveMembers;

  /**
   * Returns the response version.
   *
   * @return The response version.
   */
  public long version() {
    return version;
  }

  /**
   * Returns the active members list.
   *
   * @return The active members list.
   */
  public Collection<Member> activeMembers() {
    return activeMembers;
  }

  /**
   * Returns the passive members list.
   *
   * @return The passive members list.
   */
  public Collection<Member> passiveMembers() {
    return passiveMembers;
  }

  /**
   * Returns the reserve members list.
   *
   * @return The reserve members list.
   */
  public Collection<Member> reserveMembers() {
    return reserveMembers;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      version = buffer.readLong();
      activeMembers = serializer.readObject(buffer);
      passiveMembers = serializer.readObject(buffer);
      reserveMembers = serializer.readObject(buffer);
    } else {
      int errorCode = buffer.readByte();
      if (errorCode != 0) {
        error = RaftError.forId(errorCode);
      }
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeByte(status.id());
    if (status == Status.OK) {
      buffer.writeLong(version);
      serializer.writeObject(activeMembers, buffer);
      serializer.writeObject(passiveMembers, buffer);
      serializer.writeObject(reserveMembers, buffer);
    } else {
      buffer.writeByte(error != null ? error.id() : 0);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, version, activeMembers, passiveMembers, reserveMembers);
  }

  @Override
  public boolean equals(Object object) {
    if (getClass().isAssignableFrom(object.getClass())) {
      ConfigurationResponse response = (ConfigurationResponse) object;
      return response.status == status
        && response.version == version
        && response.activeMembers.equals(activeMembers)
        && response.passiveMembers.equals(passiveMembers)
        && response.reserveMembers.equals(reserveMembers);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, version=%d, activeMembers=%s, passiveMembers=%s, reserveMembers=%s]", getClass().getSimpleName(), status, version, activeMembers, passiveMembers, reserveMembers);
  }

  /**
   * Configuration response builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends ConfigurationResponse<U>> extends AbstractResponse.Builder<T, U> {
    protected Builder(U response) {
      super(response);
    }

    /**
     * Sets the response version.
     *
     * @param version The response version.
     * @return The response builder.
     * @throws IllegalArgumentException if {@code version} is negative
     */
    @SuppressWarnings("unchecked")
    public T withVersion(long version) {
      response.version = Assert.argNot(version, version < 0, "version cannot be negative");
      return (T) this;
    }

    /**
     * Sets the active members.
     *
     * @param members The active members.
     * @return The response builder.
     * @throws NullPointerException if {@code members} is null
     */
    @SuppressWarnings("unchecked")
    public T withActiveMembers(Collection<Member> members) {
      response.activeMembers = Assert.notNull(members, "members");
      return (T) this;
    }

    /**
     * Sets the passive members.
     *
     * @param members The passive members.
     * @return The response builder.
     * @throws NullPointerException if {@code members} is null
     */
    @SuppressWarnings("unchecked")
    public T withPassiveMembers(Collection<Member> members) {
      response.passiveMembers = Assert.notNull(members, "members");
      return (T) this;
    }

    /**
     * Sets the reserve members.
     *
     * @param members The reserve members.
     * @return The response builder.
     * @throws NullPointerException if {@code members} is null
     */
    @SuppressWarnings("unchecked")
    public T withReserveMembers(Collection<Member> members) {
      response.reserveMembers = Assert.notNull(members, "members");
      return (T) this;
    }

    /**
     * @throws IllegalStateException if active members or passive members are null
     */
    @Override
    public U build() {
      super.build();
      if (response.status == Status.OK) {
        Assert.state(response.activeMembers != null, "activeMembers cannot be null");
        Assert.state(response.passiveMembers != null, "passiveMembers cannot be null");
        Assert.state(response.reserveMembers != null, "reserveMembers cannot be null");
      }
      return response;
    }
  }

}
