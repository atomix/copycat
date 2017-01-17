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
package io.atomix.copycat.server.protocol.net.response;

import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.net.response.AbstractNetResponse;
import io.atomix.copycat.protocol.websocket.response.WebSocketResponse;
import io.atomix.copycat.server.protocol.response.AcceptResponse;

import java.util.Objects;

/**
 * Server accept client response.
 * <p>
 * Accept client responses are sent to between servers once a new
 * {@link io.atomix.copycat.server.storage.entry.ConnectEntry} has been committed to the Raft log, denoting
 * the relationship between a client and server. If the acceptance of the connection was successful, the
 * response status will be {@link WebSocketResponse.Status#OK}, otherwise an error
 * will be provided.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetAcceptResponse extends AbstractNetResponse implements AcceptResponse, RaftNetResponse {
  public NetAcceptResponse(long id, Status status, CopycatError error) {
    super(id, status, error);
  }

  @Override
  public Type type() {
    return RaftNetResponse.Types.ACCEPT_RESPONSE;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NetAcceptResponse) {
      NetAcceptResponse response = (NetAcceptResponse) object;
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
  public static class Builder extends AbstractNetResponse.Builder<AcceptResponse.Builder, AcceptResponse> implements AcceptResponse.Builder {
    public Builder(long id) {
      super(id);
    }

    @Override
    public NetAcceptResponse build() {
      return new NetAcceptResponse(id, status, error);
    }
  }

}
