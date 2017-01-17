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
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.response.ReconfigureResponse;

import java.util.Collection;

/**
 * Server configuration change response.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetReconfigureResponse extends NetConfigurationResponse implements ReconfigureResponse {
  public NetReconfigureResponse(long id, Status status, CopycatError error, long index, long term, long timestamp, Collection<Member> members) {
    super(id, status, error, index, term, timestamp, members);
  }

  @Override
  public Type type() {
    return RaftNetResponse.Types.RECONFIGURE_RESPONSE;
  }

  /**
   * Reconfigure response builder.
   */
  public static class Builder extends NetConfigurationResponse.Builder<ReconfigureResponse.Builder, ReconfigureResponse> implements ReconfigureResponse.Builder {
    public Builder(long id) {
      super(id);
    }

    @Override
    public NetReconfigureResponse build() {
      return new NetReconfigureResponse(id, status, error, index, term, timestamp, members);
    }
  }

  /**
   * Reconfigure response serializer.
   */
  public static class Serializer extends NetConfigurationResponse.Serializer<NetReconfigureResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetReconfigureResponse response) {
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
    public NetReconfigureResponse read(Kryo kryo, Input input, Class<NetReconfigureResponse> type) {
      return new NetReconfigureResponse(input.readLong(), Status.forId(input.readByte()), CopycatError.forId(input.readByte()), input.readLong(), input.readLong(), input.readLong(), kryo.readObject(input, Collection.class));
    }
  }
}
