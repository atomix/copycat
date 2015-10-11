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
package io.atomix.copycat.client.request;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.BuilderPool;
import io.atomix.copycat.client.Operation;

import java.util.function.Supplier;

/**
 * Operation request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class OperationRequest<T extends OperationRequest<T>> extends SessionRequest<T> {
  protected long sequence;

  /**
   * Returns the request sequence number.
   *
   * @return The request sequence number.
   */
  public long sequence() {
    return sequence;
  }

  /**
   * Returns the request operation.
   *
   * @return The request operation.
   */
  public abstract Operation operation();

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    sequence = buffer.readLong();
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(sequence);
  }

  /**
   * Operation request builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends OperationRequest<U>> extends SessionRequest.Builder<T, U> {

    /**
     * @throws NullPointerException if {@code pool} or {@code factory} are null
     */
    protected Builder(BuilderPool<T, U> pool, Supplier<U> factory) {
      super(pool, factory);
    }

    /**
     * Sets the request sequence number.
     *
     * @param sequence The request sequence number.
     * @return The request builder.
     * @throws IllegalArgumentException If the request sequence number is not positive.
     */
    @SuppressWarnings("unchecked")
    public T withSequence(long sequence) {
      request.sequence = Assert.argNot(sequence, sequence < 0, "sequence must be positive");
      return (T) this;
    }

    @Override
    public U build() {
      super.build();
      Assert.stateNot(request.sequence < 0, "sequence cannot be less than 0");
      return request;
    }
  }

}
