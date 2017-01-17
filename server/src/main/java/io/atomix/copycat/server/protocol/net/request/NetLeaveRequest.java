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

import io.atomix.copycat.server.cluster.Member;
import io.atomix.copycat.server.protocol.request.LeaveRequest;

/**
 * Server leave configuration request.
 * <p>
 * The leave request is the mechanism by which servers remove themselves from a cluster. When a server
 * wants to leave a cluster, it must submit a leave request to the leader. The leader will attempt to commit
 * the configuration change and, if successful, respond to the join request with the updated configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class NetLeaveRequest extends NetConfigurationRequest implements LeaveRequest {
  public NetLeaveRequest(long id, Member member) {
    super(id, member);
  }

  @Override
  public Type type() {
    return RaftNetRequest.Types.LEAVE_REQUEST;
  }

  /**
   * Leave request builder.
   */
  public static class Builder extends NetConfigurationRequest.Builder<LeaveRequest.Builder, LeaveRequest> implements LeaveRequest.Builder {
    public Builder(long id) {
      super(id);
    }

    @Override
    public NetLeaveRequest build() {
      return new NetLeaveRequest(id, member);
    }
  }

}
