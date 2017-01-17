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
import io.atomix.copycat.server.protocol.response.LeaveResponse;

import java.util.Collection;

/**
 * Server leave configuration change response.
 * <p>
 * Leave responses are sent in response to a request to add a server to the cluster configuration. If a
 * configuration change is failed due to a conflict, the response status will be
 * {@link WebSocketResponse.Status#ERROR} but the response {@link #error()} will
 * be {@code null}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetLeaveResponse extends NetConfigurationResponse implements LeaveResponse {
  public NetLeaveResponse(long id, Status status, CopycatError error, long index, long term, long timestamp, Collection<Member> members) {
    super(id, status, error, index, term, timestamp, members);
  }

  @Override
  public Type type() {
    return RaftNetResponse.Types.LEAVE_RESPONSE;
  }

  /**
   * Leave response builder.
   */
  public static class Builder extends NetConfigurationResponse.Builder<LeaveResponse.Builder, LeaveResponse> implements LeaveResponse.Builder {
    public Builder(long id) {
      super(id);
    }

    @Override
    public NetLeaveResponse build() {
      return new NetLeaveResponse(id, status, error, index, term, timestamp, members);
    }
  }

  /**
   * Leave response serializer.
   */
  public static class Serializer extends NetConfigurationResponse.Serializer<NetLeaveResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetLeaveResponse response) {
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
    public NetLeaveResponse read(Kryo kryo, Input input, Class<NetLeaveResponse> type) {
      return new NetLeaveResponse(input.readLong(), Status.forId(input.readByte()), CopycatError.forId(input.readByte()), input.readLong(), input.readLong(), input.readLong(), kryo.readObject(input, Collection.class));
    }
  }
}
