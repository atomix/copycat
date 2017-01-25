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

/**
 * Abstract local response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public abstract class AbstractResponse implements ProtocolResponse {
  protected final Status status;
  protected final ProtocolResponse.Error error;

  public AbstractResponse(Status status, ProtocolResponse.Error error) {
    this.status = Assert.notNull(status, "status");
    this.error = error;
  }

  @Override
  public Status status() {
    return status;
  }

  @Override
  public ProtocolResponse.Error error() {
    return error;
  }

  /**
   * Abstract response error.
   */
  public static class Error implements ProtocolResponse.Error {
    private final Type type;
    private final String message;

    public Error(Type type, String message) {
      this.type = type;
      this.message = message;
    }

    @Override
    public Type type() {
      return type;
    }

    @Override
    public String message() {
      return message;
    }

    @Override
    public String toString() {
      return String.format("%s[type=%s, message=%s]", getClass().getSimpleName(), type.name(), message);
    }
  }

  /**
   * Abstract response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  protected static abstract class Builder<T extends ProtocolResponse.Builder<T, U>, U extends ProtocolResponse> implements ProtocolResponse.Builder<T, U> {
    protected Status status = Status.OK;
    protected Error error;

    @Override
    @SuppressWarnings("unchecked")
    public T withStatus(Status status) {
      this.status = Assert.notNull(status, "status");
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withError(Error.Type type, String message) {
      this.error = new Error(type, message);
      return (T) this;
    }
  }
}
