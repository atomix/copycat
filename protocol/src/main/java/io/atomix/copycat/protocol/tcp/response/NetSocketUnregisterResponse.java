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
import io.atomix.copycat.protocol.response.UnregisterResponse;
import io.atomix.copycat.protocol.websocket.request.WebSocketKeepAliveRequest;
import io.atomix.copycat.protocol.websocket.request.WebSocketUnregisterRequest;

import java.util.Objects;

/**
 * Session unregister response.
 * <p>
 * Session unregister responses are sent in response to a {@link WebSocketUnregisterRequest}.
 * If the response is successful, that indicates the session was successfully unregistered. For unsuccessful
 * unregister requests, sessions can still be expired by simply haulting {@link WebSocketKeepAliveRequest}s
 * to the cluster.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetSocketUnregisterResponse extends NetSocketSessionResponse implements UnregisterResponse {
  protected NetSocketUnregisterResponse(long id, Status status, CopycatError error) {
    super(id, status, error);
  }

  @Override
  public Type type() {
    return Type.UNREGISTER_RESPONSE;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), status);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NetSocketUnregisterResponse) {
      NetSocketUnregisterResponse response = (NetSocketUnregisterResponse) object;
      return response.status == status;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[status=%s]", getClass().getSimpleName(), status);
  }

  /**
   * Status response builder.
   */
  public static class Builder extends NetSocketSessionResponse.Builder<UnregisterResponse.Builder, UnregisterResponse> implements UnregisterResponse.Builder {
    public Builder(long id) {
      super(id);
    }

    @Override
    public UnregisterResponse build() {
      return new NetSocketUnregisterResponse(id, status, error);
    }
  }

  /**
   * Unregister response serializer.
   */
  public static class Serializer extends NetSocketSessionResponse.Serializer<NetSocketUnregisterResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetSocketUnregisterResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.error == null) {
        output.writeByte(0);
      } else {
        output.writeByte(response.error.id());
      }
    }

    @Override
    public NetSocketUnregisterResponse read(Kryo kryo, Input input, Class<NetSocketUnregisterResponse> type) {
      return new NetSocketUnregisterResponse(input.readLong(), Status.forId(input.readByte()), CopycatError.forId(input.readByte()));
    }
  }
}
