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
package io.atomix.catalogue.client.request;

import io.atomix.catalyst.buffer.BufferInput;
import io.atomix.catalyst.buffer.BufferOutput;
import io.atomix.catalyst.serializer.Serializer;
import io.atomix.catalyst.util.Assert;
import io.atomix.catalyst.util.BuilderPool;
import io.atomix.catalyst.util.ReferenceFactory;
import io.atomix.catalyst.util.ReferenceManager;

/**
 * Session request.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class SessionRequest<T extends SessionRequest<T>> extends AbstractRequest<T> {
  protected long session;

  /**
   * @throws NullPointerException if {@code referenceManager} is null
   */
  public SessionRequest(ReferenceManager<T> referenceManager) {
    super(referenceManager);
  }

  /**
   * Returns the session ID.
   *
   * @return The session ID.
   */
  public long session() {
    return session;
  }

  @Override
  public void readObject(BufferInput buffer, Serializer serializer) {
    session = buffer.readLong();
  }

  @Override
  public void writeObject(BufferOutput buffer, Serializer serializer) {
    buffer.writeLong(session);
  }

  /**
   * Session request builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends SessionRequest<U>> extends AbstractRequest.Builder<T, U> {

    /**
     * @throws NullPointerException if {@code pool} or {@code factory} are null
     */
    protected Builder(BuilderPool<T, U> pool, ReferenceFactory<U> factory) {
      super(pool, factory);
    }

    @Override
    protected void reset() {
      super.reset();
      request.session = 0;
    }

    /**
     * Sets the session ID.
     *
     * @param session The session ID.
     * @return The request builder.
     * @throws IllegalArgumentException if {@code session} is less than 0
     */
    @SuppressWarnings("unchecked")
    public T withSession(long session) {
      request.session = Assert.argNot(session, session < 0, "session must not be negative");
      return (T) this;
    }

    @Override
    public U build() {
      super.build();
      Assert.stateNot(request.session < 1, "session cannot be less than 1");
      return request;
    }
  }

}
