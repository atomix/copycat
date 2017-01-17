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
package io.atomix.copycat.server.protocol.net.request;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.request.JoinRequest;

/**
 * Server join configuration change request.
 * <p>
 * The join request is the mechanism by which new servers join a cluster. When a server wants to
 * join a cluster, it must submit a join request to the leader. The leader will attempt to commit
 * the configuration change and, if successful, respond to the join request with the updated configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetJoinRequest extends NetConfigurationRequest implements JoinRequest {
  public NetJoinRequest(long id, Member member) {
    super(id, member);
  }

  @Override
  public Type type() {
    return RaftNetRequest.Types.JOIN_REQUEST;
  }

  /**
   * Join request builder.
   */
  public static class Builder extends NetConfigurationRequest.Builder<JoinRequest.Builder, JoinRequest> implements JoinRequest.Builder {
    public Builder(long id) {
      super(id);
    }

    @Override
    public JoinRequest build() {
      return new NetJoinRequest(id, member);
    }
  }

  /**
   * Join request serializer.
   */
  public static class Serializer extends NetConfigurationRequest.Serializer<NetJoinRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetJoinRequest request) {
      output.writeLong(request.id);
      kryo.writeClassAndObject(output, request.member);
    }

    @Override
    public NetJoinRequest read(Kryo kryo, Input input, Class<NetJoinRequest> type) {
      return new NetJoinRequest(input.readLong(), (Member) kryo.readClassAndObject(input));
    }
  }
}
