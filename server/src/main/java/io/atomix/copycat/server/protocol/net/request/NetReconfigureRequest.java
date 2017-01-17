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
import io.atomix.catalyst.util.Assert;
import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.request.ReconfigureRequest;

import java.util.Objects;

/**
 * Member configuration change request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetReconfigureRequest extends NetConfigurationRequest implements ReconfigureRequest {
  private final long index;
  private final long term;

  public NetReconfigureRequest(long id, Member member, long index, long term) {
    super(id, member);
    this.index = index;
    this.term = term;
  }

  @Override
  public Type type() {
    return RaftNetRequest.Types.RECONFIGURE_REQUEST;
  }

  @Override
  public long index() {
    return index;
  }

  @Override
  public long term() {
    return term;
  }

  @Override
  public int hashCode() {
    return Objects.hash(getClass(), index, member);
  }

  @Override
  public boolean equals(Object object) {
    if (object instanceof NetReconfigureRequest) {
      NetReconfigureRequest request = (NetReconfigureRequest) object;
      return request.index == index && request.term == term && request.member.equals(member);
    }
    return false;
  }

  @Override
  public String toString() {
    return String.format("%s[index=%d, term=%d, member=%s]", getClass().getSimpleName(), index, term, member);
  }

  /**
   * Reconfigure request builder.
   */
  public static class Builder extends NetConfigurationRequest.Builder<ReconfigureRequest.Builder, ReconfigureRequest> implements ReconfigureRequest.Builder {
    private long index;
    private long term;

    public Builder(long id) {
      super(id);
    }

    @Override
    public Builder withIndex(long index) {
      this.index = Assert.argNot(index, index < 0, "index must be positive");
      return this;
    }

    @Override
    public Builder withTerm(long term) {
      this.term = Assert.argNot(term, term < 0, "term must be positive");
      return this;
    }

    @Override
    public ReconfigureRequest build() {
      return new NetReconfigureRequest(id, member, index, term);
    }
  }

  /**
   * Reconfigure request serializer.
   */
  public static class Serializer extends NetConfigurationRequest.Serializer<NetReconfigureRequest> {
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
