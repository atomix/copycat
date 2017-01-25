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
import io.atomix.copycat.server.protocol.request.ConfigureRequest;

import java.util.Collection;
import java.util.List;

/**
 * TCP configure request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetConfigureRequest extends ConfigureRequest implements RaftNetRequest<NetConfigureRequest> {
  private final long id;

  public NetConfigureRequest(long id, long term, int leader, long index, long timestamp, Collection<Member> members) {
    super(term, leader, index, timestamp, members);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Type.CONFIGURE;
  }

  /**
   * TCP configure request builder.
   */
  public static class Builder extends ConfigureRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public ConfigureRequest copy(ConfigureRequest request) {
      return new NetConfigureRequest(id, request.term(), request.leader(), request.index(), request.timestamp(), request.members());
    }

    @Override
    public ConfigureRequest build() {
      return new NetConfigureRequest(id, term, leader, index, timestamp, members);
    }
  }

  /**
   * Configure request serializer.
   */
  public static class Serializer extends RaftNetRequest.Serializer<NetConfigureRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetConfigureRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.term);
      output.writeInt(request.leader);
      output.writeLong(request.index);
      output.writeLong(request.timestamp);
      kryo.writeClassAndObject(output, request.members);
    }

    @Override
    @SuppressWarnings("unchecked")
    public NetConfigureRequest read(Kryo kryo, Input input, Class<NetConfigureRequest> type) {
      return new NetConfigureRequest(input.readLong(), input.readLong(), input.readInt(), input.readLong(), input.readLong(), (List) kryo.readClassAndObject(input));
    }
  }
}
