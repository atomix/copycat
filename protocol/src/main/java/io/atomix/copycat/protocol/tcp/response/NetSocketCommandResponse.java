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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.response.CommandResponse;
import io.atomix.copycat.protocol.websocket.request.WebSocketCommandRequest;

/**
 * Client command response.
 * <p>
 * Command responses are sent by servers to clients upon the completion of a
 * {@link WebSocketCommandRequest}. Command responses are sent with the
 * {@link #index()} (or index) of the state machine at the point at which the command was evaluated.
 * This can be used by the client to ensure it sees state progress monotonically. Note, however, that
 * command responses may not be sent or received in sequential order. If a command response has to await
 * the completion of an event, or if the response is proxied through another server, responses may be
 * received out of order. Clients should resequence concurrent responses to ensure they're handled in FIFO order.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetSocketCommandResponse extends NetSocketOperationResponse implements CommandResponse {
  protected NetSocketCommandResponse(long id, Status status, CopycatError error, long index, long eventIndex, Object result) {
    super(id, status, error, index, eventIndex, result);
  }

  @Override
  public Type type() {
    return Type.COMMAND_RESPONSE;
  }

  /**
   * Command response builder.
   */
  public static class Builder extends NetSocketOperationResponse.Builder<CommandResponse.Builder, CommandResponse> implements CommandResponse.Builder {
    public Builder(long id) {
      super(id);
    }

    @Override
    public CommandResponse build() {
      return new NetSocketCommandResponse(id, status, error, index, eventIndex, result);
    }
  }

  /**
   * Command response serializer.
   */
  public static class Serializer extends NetSocketOperationResponse.Serializer<NetSocketCommandResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetSocketCommandResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.error == null) {
        output.writeByte(0);
      } else {
        output.writeByte(response.error.id());
      }
      output.writeLong(response.index);
      output.writeLong(response.eventIndex);
      kryo.writeClassAndObject(output, response.result);
    }

    @Override
    public NetSocketCommandResponse read(Kryo kryo, Input input, Class<NetSocketCommandResponse> type) {
      return new NetSocketCommandResponse(input.readLong(), Status.forId(input.readByte()), CopycatError.forId(input.readByte()), input.readLong(), input.readLong(), kryo.readClassAndObject(input));
    }
  }
}
