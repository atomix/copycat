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
  protected Collection<Member> members;

  /**
   * Returns the response version.
   *
   * @return The response version.
   */
  public long version() {
    return version;
  }

  /**
   * Returns the configuration members list.
   *
   * @return The configuration members list.
   */
  public Collection<Member> members() {
    return members;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    if (status == Status.OK) {
      error = null;
      version = buffer.readLong();
      members = serializer.readObject(buffer);
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
      serializer.writeObject(members, buffer);
    } else {
      buffer.writeByte(error != null ? error.id() : 0);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, version, members);
  }

  @Override
  public boolean equals(Object object) {
    if (getClass().isAssignableFrom(object.getClass())) {
      ConfigurationResponse response = (ConfigurationResponse) object;
      return response.status == status
        && response.version == version
        && response.members.equals(members);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, version=%d, members=%s]", getClass().getSimpleName(), status, version, members);
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
     * Sets the response members.
     *
     * @param members The response members.
     * @return The response builder.
     * @throws NullPointerException if {@code members} is null
     */
    @SuppressWarnings("unchecked")
    public T withMembers(Collection<Member> members) {
      response.members = Assert.notNull(members, "members");
      return (T) this;
    }

    @Override
    public int hashCode() {
      return Objects.hash(response);
    }

    @Override
    public boolean equals(Object object) {
      return getClass().isAssignableFrom(object.getClass()) && ((Builder) object).response.equals(response);
    }

    /**
     * @throws IllegalStateException if active members or passive members are null
     */
    @Override
    public U build() {
      super.build();
      if (response.status == Status.OK) {
        Assert.state(response.members != null, "activeMembers members cannot be null");
      }
      return response;
    }

    @Override
    public String toString() {
      return String.format("%s[response=%s]", getClass().getCanonicalName(), response);
    }

  }

}
