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
package io.atomix.copycat.server.protocol.response;

import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.response.AbstractResponse;
import io.atomix.copycat.protocol.response.ProtocolResponse;

import java.util.Objects;

/**
 * Server accept client response.
 * <p>
 * Accept client responses are sent to between servers once a new
 * {@link io.atomix.copycat.server.storage.entry.ConnectEntry} has been committed to the Raft log, denoting
 * the relationship between a client and server. If the acceptance of the connection was successful, the
 * response status will be {@link Status#OK}, otherwise an error
 * will be provided.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class AcceptResponse extends AbstractResponse {
  public AcceptResponse(ProtocolResponse.Status status, CopycatError error) {
    super(status, error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof AcceptResponse) {
      AcceptResponse response = (AcceptResponse) object;
      return response.status == status;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s]", getClass().getSimpleName(), status);
  }

  /**
   * Register response builder.
   */
  public static class Builder extends AbstractResponse.Builder<AcceptResponse.Builder, AcceptResponse> {
    @Override
    public AcceptResponse copy(AcceptResponse response) {
      return new AcceptResponse(response.status, response.error);
    }

    @Override
    public AcceptResponse build() {
      return new AcceptResponse(status, error);
    }
  }
}
