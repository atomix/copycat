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
import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.server.protocol.response.InstallResponse;

/**
 * TCP install response.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public class NetInstallResponse extends InstallResponse implements RaftNetResponse {
  private final long id;

  public NetInstallResponse(long id, ProtocolResponse.Status status, CopycatError error) {
    super(status, error);
    this.id = id;
  }

  @Override
  public long id() {
    return id;
  }

  @Override
  public Type type() {
    return Types.INSTALL_RESPONSE;
  }

  /**
   * TCP install response builder.
   */
  public static class Builder extends InstallResponse.Builder {
    private final long id;

    public Builder(long id) {
      this.id = id;
    }

    @Override
    public InstallResponse build() {
      return new NetInstallResponse(id, status, error);
    }
  }

  /**
   * Install response serializer.
   */
  public static class Serializer extends RaftNetResponse.Serializer<NetInstallResponse> {
    @Override
    public void write(Kryo kryo, Output output, NetInstallResponse response) {
      output.writeLong(response.id);
      output.writeByte(response.status.id());
      if (response.error == null) {
        output.writeByte(0);
      } else {
        output.writeByte(response.error.id());
      }
    }

    @Override
    public NetInstallResponse read(Kryo kryo, Input input, Class<NetInstallResponse> type) {
      return new NetInstallResponse(input.readLong(), ProtocolResponse.Status.forId(input.readByte()), CopycatError.forId(input.readByte()));
    }
  }
}
