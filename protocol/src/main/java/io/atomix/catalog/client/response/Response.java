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
package io.atomix.catalog.client.response;

import io.atomix.catalog.client.error.RaftError;
import io.atomix.catalyst.serializer.CatalystSerializable;
import io.atomix.catalyst.util.BuilderPool;
import io.atomix.catalyst.util.ReferenceCounted;

/**
 * Protocol response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface Response<T extends Response<T>> extends ReferenceCounted<T>, CatalystSerializable {

  /**
   * Response status.
   */
  enum Status {

    /**
     * Indicates a successful response status.
     */
    OK(1),

    /**
     * Indicates a response containing an error.
     */
    ERROR(0);

    /**
     * Returns the status for the given identifier.
     *
     * @param id The status identifier.
     * @return The status for the given identifier.
     * @throws IllegalArgumentException if {@code id} is not 0 or 1
     */
    public static Status forId(int id) {
      switch (id) {
        case 1:
          return OK;
        case 0:
          return ERROR;
      }
      throw new IllegalArgumentException("invalid status identifier: " + id);
    }

    private final byte id;

    Status(int id) {
      this.id = (byte) id;
    }

    /**
     * Returns the status identifier.
     *
     * @return The status identifier.
     */
    public byte id() {
      return id;
    }

  }

  /**
   * Returns the response status.
   *
   * @return The response status.
   */
  Status status();

  /**
   * Returns the response error if the response status is {@code Status.ERROR}
   *
   * @return The response error.
   */
  RaftError error();

  /**
   * Response builder.
   *
   * @param <T> The builder type.
   * @param <U> The response type.
   */
  abstract class Builder<T extends Builder<T, U>, U extends Response> extends io.atomix.catalyst.util.Builder<U> {

    protected Builder(BuilderPool pool) {
      super(pool);
    }

    /**
     * Sets the response status.
     *
     * @param status The response status.
     * @return The response builder.
     * @throws NullPointerException if {@code status} is null
     */
    public abstract T withStatus(Status status);

    /**
     * Sets the response error.
     *
     * @param error The response error.
     * @return The response builder.
     * @throws NullPointerException if {@code error} is null
     */
    public abstract T withError(RaftError error);

  }

}
