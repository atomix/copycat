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
import io.atomix.copycat.server.cluster.Member;

import java.util.Collection;
import java.util.Objects;

/**
 * Server configuration response.
 * <p>
 * Configuration responses are sent in response to configuration change requests once a configuration
 * change is completed or fails. Note that configuration changes can frequently fail due to the limitation
 * of commitment of configuration changes. No two configuration changes may take place simultaneously. If a
 * configuration change is failed due to a conflict, the response status will be
 * {@link io.atomix.copycat.client.response.Response.Status#ERROR} but the response {@link #error()} will
 * be {@code null}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ConfigurationResponse extends AbstractResponse {
  protected long index;
  protected Collection<Member> members;

  /**
   * Returns the response index.
   *
   * @return The response index.
   */
  public long index() {
    return index;
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
      index = buffer.readLong();
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
      buffer.writeLong(index);
      serializer.writeObject(members, buffer);
    } else {
      buffer.writeByte(error != null ? error.id() : 0);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, index, members);
  }

  @Override
  public boolean equals(Object object) {
    if (getClass().isAssignableFrom(object.getClass())) {
      ConfigurationResponse response = (ConfigurationResponse) object;
      return response.status == status
        && response.index == index
        && response.members.equals(members);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, index=%d, members=%s]", getClass().getSimpleName(), status, index, members);
  }

  /**
   * Configuration response builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends ConfigurationResponse> extends AbstractResponse.Builder<T, U> {
    protected Builder(U response) {
      super(response);
    }

    /**
     * Sets the response index.
     *
     * @param index The response index.
     * @return The response builder.
     * @throws IllegalArgumentException if {@code index} is negative
     */
    @SuppressWarnings("unchecked")
    public T withIndex(long index) {
      response.index = Assert.argNot(index, index < 0, "index cannot be negative");
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
