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
}
