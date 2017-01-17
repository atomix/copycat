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
package io.atomix.copycat.protocol.tcp.request;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.protocol.request.KeepAliveRequest;
import io.atomix.copycat.protocol.websocket.response.WebSocketRegisterResponse;

import java.util.Objects;

/**
 * Session keep alive request.
 * <p>
 * Keep alive requests are sent by clients to servers to maintain a session registered via
 * a {@link NetSocketRegisterRequest}. Once a session has been registered, clients are responsible for sending
 * keep alive requests to the cluster at a rate less than the provided {@link WebSocketRegisterResponse#timeout()}.
 * Keep alive requests also server to acknowledge the receipt of responses and events by the client.
 * The {@link #commandSequence()} number indicates the highest command sequence number for which the client
 * has received a response, and the {@link #eventIndex()} number indicates the highest index for which the
 * client has received an event in proper sequence.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetSocketKeepAliveRequest extends NetSocketSessionRequest implements KeepAliveRequest {
  private final long commandSequence;
  private final long eventIndex;

  protected NetSocketKeepAliveRequest(long id, long session, long commandSequence, long eventIndex) {
    super(id, session);
    this.commandSequence = commandSequence;
    this.eventIndex = eventIndex;
  }

  @Override
  public Type type() {
    return Type.KEEP_ALIVE_REQUEST;
  }

  @Override
  public long commandSequence() {
    return commandSequence;
  }

  @Override
  public long eventIndex() {
    return eventIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session, commandSequence);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NetSocketKeepAliveRequest) {
      NetSocketKeepAliveRequest request = (NetSocketKeepAliveRequest) object;
      return request.session == session
        && request.commandSequence == commandSequence
        && request.eventIndex == eventIndex;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d, commandSequence=%d, eventIndex=%d]", getClass().getSimpleName(), session, commandSequence, eventIndex);
  }

  /**
   * Keep alive request builder.
   */
  public static class Builder extends NetSocketSessionRequest.Builder<KeepAliveRequest.Builder, KeepAliveRequest> implements KeepAliveRequest.Builder {
    private long commandSequence;
    private long eventIndex;

    public Builder(long id) {
      super(id);
    }

    @Override
    public Builder withCommandSequence(long commandSequence) {
      this.commandSequence = Assert.argNot(commandSequence, commandSequence < 0, "commandSequence cannot be negative");
      return this;
    }

    @Override
    public Builder withEventIndex(long eventIndex) {
      this.eventIndex = Assert.argNot(eventIndex, eventIndex < 0, "eventIndex cannot be negative");
      return this;
    }

    @Override
    public KeepAliveRequest build() {
      return new NetSocketKeepAliveRequest(id, session, commandSequence, eventIndex);
    }
  }

  /**
   * Keep-alive request serializer.
   */
  public static class Serializer extends NetSocketSessionRequest.Serializer<NetSocketKeepAliveRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetSocketKeepAliveRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.session);
      output.writeLong(request.commandSequence);
      output.writeLong(request.eventIndex);
    }

    @Override
    public NetSocketKeepAliveRequest read(Kryo kryo, Input input, Class<NetSocketKeepAliveRequest> type) {
      return new NetSocketKeepAliveRequest(input.readLong(), input.readLong(), input.readLong(), input.readLong());
    }
  }
}
