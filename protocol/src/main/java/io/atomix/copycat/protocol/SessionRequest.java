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

/**
 * Base session request.
 * <p>
 * This is the base request for session-related requests. Many client requests are handled within the
 * context of a {@link #session()} identifier.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class SessionRequest extends AbstractRequest {
  protected long session;
  protected boolean forwarded;

  /**
   * Returns the session ID.
   *
   * @return The session ID.
   */
  public long session() {
    return session;
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
    super.readObject(buffer, serializer);
    session = buffer.readLong();
    forwarded = buffer.readBoolean();
  }

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
    super.writeObject(buffer, serializer);
    buffer.writeLong(session);
    buffer.writeBoolean(forwarded);
  }

  public void setForwarded() {
    forwarded = true;
  }

  public boolean wasForwarded() {
    return forwarded;
  }

  /**
   * Session request builder.
   */
  public static abstract class Builder<T extends Builder<T, U>, U extends SessionRequest> extends AbstractRequest.Builder<T, U> {
    protected Builder(U request) {
      super(request);
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
      request.session = Assert.argNot(session, session < 1, "session cannot be less than 1");
      return (T) this;
    }

    public T forwarded() {
      request.forwarded = true;
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
