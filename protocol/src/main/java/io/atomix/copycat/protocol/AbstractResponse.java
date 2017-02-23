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
 * Base response for all client responses.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractResponse implements Response {
  protected Status status = Status.OK;
  protected CopycatError error;

  @Override
  public void writeObject(BufferOutput<?> buffer, Serializer serializer) {
  }

  @Override
  public void readObject(BufferInput<?> buffer, Serializer serializer) {
  }

  @Override
  public Status status() {
    return status;
  }

  @Override
  public CopycatError error() {
    return error;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, error=%s]", getClass().getCanonicalName(), status, error);
  }

  /**
   * Abstract response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  protected static abstract class Builder<T extends Builder<T, U>, U extends AbstractResponse> implements Response.Builder<T, U> {
    protected U response;

    /**
     * @throws NullPointerException if {@code factory} is null
     */
    protected Builder(U response) {
      this.response = response;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withStatus(Status status) {
      response.status = Assert.notNull(status, "status");
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withError(CopycatError error) {
      response.error = Assert.notNull(error, "error");
      return (T) this;
    }

    /**
     * @throws IllegalStateException if status is null
     */
    @Override
    public U build() {
      Assert.stateNot(response.status == null, "status cannot be null");
      return response;
    }

    @Override
    public int hashCode() {
      return Objects.hash(response);
    }

    @Override
    public boolean equals(Object object) {
      return getClass().isAssignableFrom(object.getClass()) && ((Builder) object).response.equals(response);
    }

    @Override
    public String toString() {
      return String.format("%s[response=%s]", getClass().getCanonicalName(), response);
    }
  }

}
