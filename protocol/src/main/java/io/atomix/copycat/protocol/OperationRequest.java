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
import io.atomix.copycat.Operation;

/**
 * Client operation request.
 * <p>
 * Operation requests are sent by clients to servers to execute operations on the replicated state
 * machine. Each operation request must be sequenced with a {@link #sequence()} number. All operations
 * will be applied to replicated state machines in the sequence in which they were sent by the client.
 * Sequence numbers must always be sequential, and in the event that an operation request fails, it must
 * be resent by the client.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class OperationRequest extends SessionRequest {
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
  public static abstract class Builder<T extends Builder<T, U>, U extends OperationRequest> extends SessionRequest.Builder<T, U> {
    protected Builder(U request) {
      super(request);
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
