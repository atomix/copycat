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
package io.atomix.copycat.protocol.tcp.response;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.response.ProtocolResponse;

import java.util.Objects;

/**
 * Base response for all client responses.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public abstract class AbstractNetSocketResponse implements NetSocketResponse {
  @JsonProperty("id")
  protected final long id;
  @JsonProperty("status")
  protected final Status status;
  @JsonProperty("error")
  protected final CopycatError error;

  protected AbstractNetSocketResponse(long id, Status status, CopycatError error) {
    this.id = id;
    this.status = status;
    this.error = error;
  }

  @Override
  @JsonGetter("id")
  public long id() {
    return id;
  }

  @Override
  @JsonGetter("status")
  public Status status() {
    return status;
  }

  @Override
  @JsonGetter("error")
  public CopycatError error() {
    return error;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s]", getClass().getCanonicalName(), status);
  }

  /**
   * Abstract response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  protected static abstract class Builder<T extends ProtocolResponse.Builder<T, U>, U extends ProtocolResponse> implements ProtocolResponse.Builder<T, U> {
    protected long id;
    protected Status status = Status.OK;
    protected CopycatError error;

    public Builder(long id) {
      this.id = id;
    }

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

    @Override
    public int hashCode() {
      return Objects.hash(id);
    }

    @Override
    public boolean equals(Object object) {
      return getClass().isAssignableFrom(object.getClass()) && ((Builder) object).id == id;
    }

    @Override
    public String toString() {
      return String.format("%s[response=%s]", getClass().getCanonicalName(), id);
    }
  }

  /**
   * Net socket response serializer.
   */
  public static abstract class Serializer<T extends AbstractNetSocketResponse> extends com.esotericsoftware.kryo.Serializer<T> {
  }
}
