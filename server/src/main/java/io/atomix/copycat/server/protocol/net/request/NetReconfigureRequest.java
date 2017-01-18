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
import io.atomix.copycat.server.protocol.request.ReconfigureRequest;

/**
 * TCP reconfigure request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetReconfigureRequest extends ReconfigureRequest implements RaftNetRequest {
  private final long id;

  public NetReconfigureRequest(long id, Member member, long index, long term) {
    super(member, index, term);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Types.RECONFIGURE_REQUEST;
  }

  /**
   * TCP reconfigure request builder.
   */
  public static class Builder extends ReconfigureRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public ReconfigureRequest copy(ReconfigureRequest request) {
      return new NetReconfigureRequest(id, request.member(), request.index(), request.term());
    }

    @Override
    public ReconfigureRequest build() {
      return new NetReconfigureRequest(id, member, index, term);
    }
  }

  /**
   * Reconfigure request serializer.
   */
  public static class Serializer extends RaftNetRequest.Serializer<NetReconfigureRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetReconfigureRequest request) {
      output.writeLong(request.id);
      kryo.writeClassAndObject(output, request.member);
      output.writeLong(request.index);
      output.writeLong(request.term);
    }

    @Override
    public NetReconfigureRequest read(Kryo kryo, Input input, Class<NetReconfigureRequest> type) {
      return new NetReconfigureRequest(input.readLong(), (Member) kryo.readClassAndObject(input), input.readLong(), input.readLong());
    }
  }
}
