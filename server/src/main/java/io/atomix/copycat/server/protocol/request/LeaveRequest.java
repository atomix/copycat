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
package io.atomix.copycat.server.protocol.request;

import io.atomix.copycat.server.cluster.Member;

/**
 * Server leave configuration request.
 * <p>
 * The leave request is the mechanism by which servers remove themselves from a cluster. When a server
 * wants to leave a cluster, it must submit a leave request to the leader. The leader will attempt to commit
 * the configuration change and, if successful, respond to the join request with the updated configuration.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LeaveRequest extends ConfigurationRequest {
  public LeaveRequest(Member member) {
    super(member);
  }

  /**
   * Leave request builder.
   */
  public static class Builder extends ConfigurationRequest.Builder<LeaveRequest.Builder, LeaveRequest> {
    @Override
    public LeaveRequest copy(LeaveRequest request) {
      return new LeaveRequest(request.member);
    }

    @Override
    public LeaveRequest build() {
      return new LeaveRequest(member);
    }
  }
}
