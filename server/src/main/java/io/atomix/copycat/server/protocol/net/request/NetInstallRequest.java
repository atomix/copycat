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
import io.atomix.copycat.server.protocol.request.InstallRequest;

/**
 * TCP install request.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetInstallRequest extends InstallRequest implements RaftNetRequest {
  private final long id;

  public NetInstallRequest(long id, long term, int leader, long index, int offset, byte[] data, boolean complete) {
    super(term, leader, index, offset, data, complete);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Types.INSTALL_REQUEST;
  }

  /**
   * TCP install request builder.
   */
  public static class Builder extends InstallRequest.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public InstallRequest copy(InstallRequest request) {
      return new NetInstallRequest(id, request.term(), request.leader(), request.index(), request.offset(), request.data(), request.complete());
    }

    @Override
    public InstallRequest build() {
      return new NetInstallRequest(id, term, leader, index, offset, data, complete);
    }
  }

  /**
   * Install request serializer.
   */
  public static class Serializer extends RaftNetRequest.Serializer<NetInstallRequest> {
    @Override
    public void write(Kryo kryo, Output output, NetInstallRequest request) {
      output.writeLong(request.id);
      output.writeLong(request.term);
      output.writeInt(request.leader);
      output.writeLong(request.index);
      output.writeInt(request.offset);
      output.writeInt(request.data.length);
      output.write(request.data);
      output.writeBoolean(request.complete);
    }

    @Override
    public NetInstallRequest read(Kryo kryo, Input input, Class<NetInstallRequest> type) {
      return new NetInstallRequest(input.readLong(), input.readLong(), input.readInt(), input.readLong(), input.readInt(), input.readBytes(input.readInt()), input.readBoolean());
    }
  }
}
