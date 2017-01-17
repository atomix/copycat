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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.error.CopycatError;
import io.atomix.copycat.protocol.websocket.response.WebSocketResponse;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.response.JoinResponse;

import java.util.Collection;

/**
 * Server join configuration change response.
 * <p>
 * Join responses are sent in response to a request to add a server to the cluster configuration. If a
 * configuration change is failed due to a conflict, the response status will be
 * {@link WebSocketResponse.Status#ERROR} but the response {@link #error()} will
 * be {@code null}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetJoinResponse extends NetConfigurationResponse implements JoinResponse {
  public NetJoinResponse(long id, Status status, CopycatError error, long index, long term, long timestamp, Collection<Member> members) {
    super(id, status, error, index, term, timestamp, members);
  }

  @Override
  public Type type() {
    return RaftNetResponse.Types.JOIN_RESPONSE;
  }

  /**
   * Join response builder.
   */
  public static class Builder extends NetConfigurationResponse.Builder<JoinResponse.Builder, JoinResponse> implements JoinResponse.Builder {
    public Builder(long id) {
      super(id);
    }

    @Override
    public NetJoinResponse build() {
      return new NetJoinResponse(id, status, error, index, term, timestamp, members);
    }
  }

  /**
   * Join response serializer.
   */
  public static class Serializer extends NetConfigurationResponse.Serializer<NetJoinResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetJoinResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.error == null) {
        output.writeByte(0);
      } else {
        output.writeByte(response.error.id());
      }
      output.writeLong(response.index);
      output.writeLong(response.term);
      output.writeLong(response.timestamp);
      kryo.writeObject(output, response.members);
    }

    @Override
    @SuppressWarnings("unchecked")
    public NetJoinResponse read(Kryo kryo, Input input, Class<NetJoinResponse> type) {
      return new NetJoinResponse(input.readLong(), Status.forId(input.readByte()), CopycatError.forId(input.readByte()), input.readLong(), input.readLong(), input.readLong(), kryo.readObject(input, Collection.class));
    }
  }
}
