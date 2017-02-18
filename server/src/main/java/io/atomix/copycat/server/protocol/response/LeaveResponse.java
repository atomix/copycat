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
package io.atomix.copycat.server.protocol.response;

import io.atomix.copycat.protocol.response.ProtocolResponse;
import io.atomix.copycat.server.cluster.Member;

import java.util.Collection;

/**
 * Server leave configuration change response.
 * <p>
 * Leave responses are sent in response to a request to add a server to the cluster configuration. If a
 * configuration change is failed due to a conflict, the response status will be
 * {@link Status#ERROR} but the response {@link #error()} will
 * be {@code null}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaveResponse extends ConfigurationResponse {

  /**
   * Returns a new leave response builder.
   *
   * @return A new leave response builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  public LeaveResponse(Status status, ProtocolResponse.Error error, long index, long term, long timestamp, Collection<Member> members) {
    super(status, error, index, term, timestamp, members);
  }

  @Override
  public RaftProtocolResponse.Type type() {
    return RaftProtocolResponse.Type.LEAVE;
  }

  /**
   * Leave response builder.
   */
  public static class Builder extends ConfigurationResponse.Builder<LeaveResponse.Builder, LeaveResponse> {
    @Override
    public LeaveResponse copy(LeaveResponse response) {
      return new LeaveResponse(response.status, response.error, response.index, response.term, response.timestamp, response.members);
    }

    @Override
    public LeaveResponse build() {
      return new LeaveResponse(status, error, index, term, timestamp, members);
    }
  }
}
