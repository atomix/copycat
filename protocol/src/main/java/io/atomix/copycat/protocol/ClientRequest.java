/*
 * Copyright 2017-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * Base client request.
 * <p>
 * This is the base request for client-related requests. Many client requests are handled within the
 * context of a {@link #client()} identifier.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class ClientRequest extends AbstractRequest {
  protected long client;

  /**
   * Returns the client ID.
   *
   * @return The client ID.
   */
  public long client() {
    return client;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    client = buffer.readLong();
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    buffer.writeLong(client);
  }

  /**
   * Session request builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends ClientRequest> extends AbstractRequest.Builder<T, U> {
    protected Builder(U request) {
      super(request);
    }

    /**
     * Sets the client ID.
     *
     * @param client The client ID.
     * @return The request builder.
     * @throws NullPointerException if {@code client} is null
     */
    @SuppressWarnings("unchecked")
    public T withClient(long client) {
      request.client = Assert.argNot(client, client <= 0, "client must be positive");
      return (T) this;
    }

    @Override
    public U build() {
      super.build();
      Assert.argNot(request.client <= 0, "client must be positive");
      return request;
    }
  }

}
