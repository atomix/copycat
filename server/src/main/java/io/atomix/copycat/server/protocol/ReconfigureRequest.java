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
package io.atomix.copycat.server.protocol;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;

import java.util.Objects;

/**
 * Member configuration change request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class ReconfigureRequest extends ConfigurationRequest {

  /**
   * Returns a new reconfigure request builder.
   *
   * @return A new reconfigure request builder.
   */
  public static Builder builder() {
    return new Builder(new ReconfigureRequest());
  }

  /**
   * Returns a new reconfigure request builder.
   *
   * @param request The request to build.
   * @return A new reconfigure request builder.
   */
  public static Builder builder(ReconfigureRequest request) {
    return new Builder(request);
  }

  private long index;
  private long term;

  /**
   * Returns the configuration index.
   *
   * @return The configuration index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the configuration term.
   *
   * @return The configuration term.
   */
  public long term() {
    return term;
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(index).writeLong(term);
    super.writeObject(buffer, serializer);
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    index = buffer.readLong();
    term = buffer.readLong();
    super.readObject(buffer, serializer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), index, member);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof ReconfigureRequest) {
      ReconfigureRequest request = (ReconfigureRequest) object;
      return request.index == index && request.term == term && request.member.equals(member);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, member=%s]", getClass().getSimpleName(), index, term, member);
  }

  /**
   * Reconfigure request builder.
   */
  public static class Builder extends ConfigurationRequest.Builder<Builder, ReconfigureRequest> {
    public Builder(ReconfigureRequest request) {
      super(request);
    }

    /**
     * Sets the request index.
     *
     * @param index The request index.
     * @return The request builder.
     */
    public Builder withIndex(long index) {
      request.index = Assert.argNot(index, index < 0, "index must be positive");
      return this;
    }

    /**
     * Sets the request term.
     *
     * @param term The request term.
     * @return The request builder.
     */
    public Builder withTerm(long term) {
      request.term = Assert.argNot(term, term < 0, "term must be positive");
      return this;
    }
  }

}
