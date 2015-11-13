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
package io.atomix.copycat.server.request;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.SerializeWith;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.client.request.AbstractRequest;

import java.util.Objects;

/**
 * Protocol heartbeat request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
@SerializeWith(id=217)
public class HeartbeatRequest extends AbstractRequest<HeartbeatRequest> {
  protected int member;

  /**
   * Returns the heartbeat member.
   *
   * @return The heartbeat member.
   */
  public int member() {
    return member;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeInt(member);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    member = buffer.readInt();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), member);
  }

  @Override
  public boolean equals(Object object) {
    return getClass().isAssignableFrom(object.getClass()) && ((HeartbeatRequest) object).member == member;
  }

  @Override
  public String toString() {
    return String.format("%s[member=%d]", getClass().getSimpleName(), member);
  }

  /**
   * Heartbeat request builder.
   */
  public static class Builder extends AbstractRequest.Builder<Builder, HeartbeatRequest> {
    protected Builder(HeartbeatRequest request) {
      super(request);
    }

    /**
     * Sets the request member.
     *
     * @param member The request member.
     * @return The request builder.
     * @throws NullPointerException if {@code member} is null
     */
    public Builder withMember(int member) {
      request.member = Assert.notNull(member, "member");
      return this;
    }

    /**
     * @throws IllegalStateException if member is null
     */
    @Override
    public HeartbeatRequest build() {
      super.build();
      Assert.state(request.member != 0, "member cannot be 0");
      return request;
    }
  }

}
