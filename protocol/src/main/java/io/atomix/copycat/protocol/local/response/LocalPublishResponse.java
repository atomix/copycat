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
import io.atomix.copycat.protocol.response.PublishResponse;

import java.util.Objects;

/**
 * Event publish response.
 * <p>
 * Publish responses are sent by clients to servers to indicate the last successful index for which
 * an event message was handled in proper sequence. If the client receives an event message out of
 * sequence, it should respond with the index of the last event it received in sequence. If an event
 * message is received in sequence, it should respond with the index of that event. Once a client has
 * responded successfully to an event message, it will be removed from memory on the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalPublishResponse extends LocalSessionResponse implements PublishResponse {
  private final long index;

  protected LocalPublishResponse(Status status, CopycatError error, long index) {
    super(status, error);
    this.index = index;
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof LocalPublishResponse) {
      LocalPublishResponse response = (LocalPublishResponse) object;
      return response.status == status
        && response.index == index;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s, error=%s, index=%d]", getClass().getSimpleName(), status, error, index);
  }

  /**
   * Publish response builder.
   */
  public static class Builder extends LocalSessionResponse.Builder<PublishResponse.Builder, PublishResponse> implements PublishResponse.Builder {
    private long index;

    @Override
    public Builder withIndex(long index) {
      this.index = Assert.argNot(index, index < 0, "index cannot be less than 0");
      return this;
    }

    @Override
    public PublishResponse build() {
      return new LocalPublishResponse(status, error, index);
    }
  }
}
