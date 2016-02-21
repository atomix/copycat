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
package io.atomix.copycat.protocol;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;

import java.util.Objects;

/**
 * Base client operation response.
 * <p>
 * All operation responses are sent with a {@link #result()} and the {@link #index()} (or index) of the state
 * machine at the point at which the operation was evaluated. The version allows clients to ensure state progresses
 * monotonically when switching servers by providing the state machine version in future operation requests.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class OperationResponse extends SessionResponse {
  protected long index;
  protected Object result;

  /**
   * Returns the query index.
   *
   * @return The query index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the operation result.
   *
   * @return The operation result.
   */
  public Object result() {
    return result;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    index = buffer.readLong();
    if (status == Status.OK) {
      error = null;
      result = serializer.readObject(buffer);
    } else {
      error = CopycatError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeByte(status.id()).writeLong(index);
    if (status == Status.OK) {
      serializer.writeObject(result, buffer);
    } else {
      buffer.writeByte(error.id());
    }
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status, result);
  }

  @Override
  public boolean equals(Object object) {
    if (getClass().isAssignableFrom(object.getClass())) {
      OperationResponse response = (OperationResponse) object;
      return response.status == status
        && response.index == index
        && ((response.result == null && result == null)
        || response.result != null && result != null && response.result.equals(result));
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, index=%d, result=%s]", getClass().getSimpleName(), status, index, result);
  }

  /**
   * Operation response builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends OperationResponse> extends SessionResponse.Builder<T, U> {
    protected Builder(U response) {
      super(response);
    }

    /**
     * Sets the response index.
     *
     * @param index The request index.
     * @return The response builder.
     * @throws IllegalArgumentException If the response index is not positive.
     */
    @SuppressWarnings("unchecked")
    public T withIndex(long index) {
      response.index = Assert.argNot(index, index < 0, "index must be positive");
      return (T) this;
    }

    /**
     * Sets the operation response result.
     *
     * @param result The response result.
     * @return The response builder.
     * @throws NullPointerException if {@code result} is null
     */
    @SuppressWarnings("unchecked")
    public T withResult(Object result) {
      response.result = result;
      return (T) this;
    }

    @Override
    public U build() {
      super.build();
      if (response.status == Status.OK) {
        Assert.stateNot(response.index < 0, "index cannot be less than 0");
      }
      return response;
    }
  }

}
