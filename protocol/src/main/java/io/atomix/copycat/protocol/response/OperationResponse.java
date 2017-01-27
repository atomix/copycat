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
package io.atomix.copycat.protocol.response;

import io.atomix.copycat.util.Assert;

import java.util.Arrays;
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
  protected final long index;
  protected final long eventIndex;
  protected final byte[] result;

  protected OperationResponse(Status status, ProtocolResponse.Error error, long index, long eventIndex, byte[] result) {
    super(status, error);
    this.index = Assert.argNot(index, index < 0, "index must be positive");
    this.eventIndex = Assert.argNot(eventIndex, eventIndex < 0, "eventIndex must be positive");
    this.result = result;
  }

  /**
   * Returns the operation index.
   *
   * @return The operation index.
   */
  public long index() {
    return index;
  }

  /**
   * Returns the event index.
   *
   * @return The event index.
   */
  public long eventIndex() {
    return eventIndex;
  }

  /**
   * Returns the operation result.
   *
   * @return The operation result.
   */
  public byte[] result() {
    return result;
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
        && response.eventIndex == eventIndex
        && ((response.result == null && result == null)
        || response.result != null && result != null
        && Arrays.equals(response.result, result));
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, index=%d, eventIndex=%d, result=byte[%d]]", getClass().getSimpleName(), status, index, eventIndex, result.length);
  }

  /**
   * Operation response builder.
   */
  public static abstract class Builder<T extends OperationResponse.Builder<T, U>, U extends OperationResponse> extends SessionResponse.Builder<T, U> {
    protected long index;
    protected long eventIndex;
    protected byte[] result;

    /**
     * Sets the response index.
     *
     * @param index The response index.
     * @return The response builder.
     * @throws IllegalArgumentException If the response index is not positive.
     */
    @SuppressWarnings("unchecked")
    public T withIndex(long index) {
      this.index = Assert.argNot(index, index < 0, "index must be positive");
      return (T) this;
    }

    /**
     * Sets the response index.
     *
     * @param eventIndex The response event index.
     * @return The response builder.
     * @throws IllegalArgumentException If the response index is not positive.
     */
    @SuppressWarnings("unchecked")
    public T withEventIndex(long eventIndex) {
      this.eventIndex = Assert.argNot(eventIndex, eventIndex < 0, "eventIndex must be positive");
      return (T) this;
    }

    /**
     * Sets the operation response result.
     *
     * @param result The response result.
     * @return The response builder.
     */
    @SuppressWarnings("unchecked")
    public T withResult(byte[] result) {
      this.result = result;
      return (T) this;
    }
  }
}
