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
import io.atomix.copycat.protocol.request.UnregisterRequest;

import java.util.Objects;

/**
 * Session unregister request.
 * <p>
 * The unregister request is sent by a client with an open session to the cluster to explicitly
 * unregister its session. Note that if a client does not send an unregister request, its session will
 * eventually expire. The unregister request simply provides a more orderly method for closing client sessions.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetSocketUnregisterRequest extends NetSocketSessionRequest implements UnregisterRequest {
  protected NetSocketUnregisterRequest(long id, long session) {
    super(id, session);
  }

  @Override
  public Type type() {
    return Type.UNREGISTER_REQUEST;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), session);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NetSocketUnregisterRequest) {
      NetSocketUnregisterRequest request = (NetSocketUnregisterRequest) object;
      return request.session == session;
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[session=%d]", getClass().getSimpleName(), session);
  }

  /**
   * Unregister request builder.
   */
  public static class Builder extends NetSocketSessionRequest.Builder<UnregisterRequest.Builder, UnregisterRequest> implements UnregisterRequest.Builder {
    public Builder(long id) {
      super(id);
    }

    @Override
    public UnregisterRequest build() {
      return new NetSocketUnregisterRequest(id, session);
    }
  }

  /**
   * Unregister request serializer.
   */
  public static class Serializer extends NetSocketSessionRequest.Serializer<NetSocketUnregisterRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetSocketUnregisterRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.session);
    }

    @Override
    public NetSocketUnregisterRequest read(Kryo kryo, Input input, Class<NetSocketUnregisterRequest> type) {
      return new NetSocketUnregisterRequest(input.readLong(), input.readLong());
    }
  }
}
