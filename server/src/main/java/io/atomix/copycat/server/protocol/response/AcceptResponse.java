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

import io.atomix.copycat.protocol.websocket.response.WebSocketResponse;

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
public interface AcceptResponse extends RaftProtocolResponse {

  /**
   * Register response builder.
   */
  interface Builder extends RaftProtocolResponse.Builder<Builder, AcceptResponse> {
  }

}
