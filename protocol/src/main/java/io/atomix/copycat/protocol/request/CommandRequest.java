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
package io.atomix.copycat.protocol.request;

import io.atomix.copycat.Command;

/**
 * Client command request.
 * <p>
 * Command requests are submitted by clients to the Copycat cluster to commit {@link Command}s to
 * the replicated state machine. Each command request must be associated with a registered
 * {@link #session()} and have a unique {@link #sequence()} number within that session. Commands will
 * be applied in the cluster in the order defined by the provided sequence number. Thus, sequence numbers
 * should never be skipped. In the event of a failure of a command request, the request should be resent
 * with the same sequence number. Commands are guaranteed to be applied in sequence order.
 * <p>
 * Command requests should always be submitted to the server to which the client is connected and will
 * be forwarded to the current cluster leader. In the event that no leader is available, the request
 * will fail and should be resubmitted by the client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public interface CommandRequest extends OperationRequest {

  /**
   * Returns the command.
   *
   * @return The command.
   */
  Command command();

  @Override
  Command operation();

  /**
   * Write request builder.
   */
  interface Builder extends OperationRequest.Builder<Builder, CommandRequest> {

    /**
     * Sets the request command.
     *
     * @param command The request command.
     * @return The request builder.
     * @throws NullPointerException if {@code command} is null
     */
    Builder withCommand(Command command);
  }

}
