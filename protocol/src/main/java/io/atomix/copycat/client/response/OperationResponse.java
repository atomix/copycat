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
package io.atomix.copycat.client.response;

import io.atomix.copycat.client.error.RaftError;
import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.BuilderPool;
import io.atomix.catalyst.util.ReferenceFactory;
import io.atomix.catalyst.util.ReferenceManager;

import java.util.Objects;

/**
 * Operation response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class OperationResponse<T extends OperationResponse<T>> extends SessionResponse<T> {
  protected long version;
  protected Object result;

  /**
   * @throws NullPointerException if {@code referenceManager} is null
   */
  public OperationResponse(ReferenceManager<T> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the query version.
   *
   * @return The query version.
   */
  public long version() {
    return version;
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
  public void readObject(BufferInput buffer, Serializer serializer) {
    status = Status.forId(buffer.readByte());
    version = buffer.readLong();
    if (status == Status.OK) {
      error = null;
      result = serializer.readObject(buffer);
    } else {
      error = RaftError.forId(buffer.readByte());
    }
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeByte(status.id()).writeLong(version);
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
        && response.version == version
        && ((response.result == null && result == null)
        || response.result != null && result != null && response.result.equals(result));
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, version=%d, result=%s]", getClass().getSimpleName(), status, version, result);
  }

  /**
   * Operation response builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends OperationResponse<U>> extends SessionResponse.Builder<T, U> {
    protected Builder(BuilderPool<T, U> pool, ReferenceFactory<U> factory) {
      super(pool, factory);
    }

    @Override
    protected void reset() {
      super.reset();
      response.version = 0;
      response.result = null;
    }

    /**
     * Sets the response version number.
     *
     * @param version The request version number.
     * @return The response builder.
     * @throws IllegalArgumentException If the response version number is not positive.
     */
    @SuppressWarnings("unchecked")
    public T withVersion(long version) {
      response.version = Assert.argNot(version, version < 0, "version must be positive");
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
        Assert.stateNot(response.version < 0, "version cannot be less than 0");
      }
      return response;
    }
  }

}
