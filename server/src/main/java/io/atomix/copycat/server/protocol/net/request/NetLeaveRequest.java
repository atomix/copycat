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
import io.atomix.copycat.server.protocol.request.LeaveRequest;

/**
 * TCP leave request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetLeaveRequest extends LeaveRequest implements RaftNetRequest {
  private final long id;

  public NetLeaveRequest(long id, Member member) {
    super(member);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Types.LEAVE_REQUEST;
  }

  /**
   * TCP leave request builder.
   */
  public static class Builder extends LeaveRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public LeaveRequest build() {
      return new NetLeaveRequest(id, member);
    }
  }

  /**
   * Leave request serializer.
   */
  public static class Serializer extends RaftNetRequest.Serializer<NetLeaveRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetLeaveRequest request) {
      output.writeLong(request.id);
      kryo.writeClassAndObject(output, request.member);
    }

    @Override
    public NetLeaveRequest read(Kryo kryo, Input input, Class<NetLeaveRequest> type) {
      return new NetLeaveRequest(input.readLong(), (Member) kryo.readClassAndObject(input));
    }
  }
}
