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
package io.atomix.copycat.protocol.local.response;

import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.response.ProtocolResponse;

/**
 * Abstract local response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class AbstractLocalResponse implements LocalResponse {
  protected final Status status;
  protected final CopycatError error;

  public AbstractLocalResponse(Status status, CopycatError error) {
    this.status = status;
    this.error = error;
  }

  @Override
  public Status status() {
    return status;
  }

  @Override
  public CopycatError error() {
    return error;
  }

  /**
   * Abstract response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  protected static abstract class Builder<T extends ProtocolResponse.Builder<T, U>, U extends ProtocolResponse> implements ProtocolResponse.Builder<T, U> {
    protected Status status = Status.OK;
    protected CopycatError error;

    @Override
    @SuppressWarnings("unchecked")
    public T withStatus(Status status) {
      this.status = Assert.notNull(status, "status");
      return (T) this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T withError(CopycatError error) {
      this.error = Assert.notNull(error, "error");
      return (T) this;
    }
  }
}
