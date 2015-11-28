/*
 * Copyright 2015 the original author or authors.
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
package io.atomix.copycat.server.state;

import io.atomix.copycat.server.controller.ServerStateController;

/**
 * Raft server state.
 *
 * @author <a href="http://github.com/kuujo>Jordan Halterman</a>
 */
public enum RaftStateType implements ServerState.Type<RaftState> {

  /**
   * Represents the state of an inactive server.
   * <p>
   * All servers start in this state and return to this state when stopped.
   */
  INACTIVE {
    @Override
    public RaftState createState(ServerStateController controller) {
      return new InactiveState(controller);
    }
  },

  /**
   * Represents the state of a server in the process of catching up its log.
   * <p>
   * Upon successfully joining an existing cluster, the server will transition to the passive state and remain there
   * until the leader determines that the server has caught up enough to be promoted to a full member.
   */
  PASSIVE {
    @Override
    public RaftState createState(ServerStateController controller) {
      return new PassiveState(controller);
    }
  },

  /**
   * Represents the state of a server participating in normal log replication.
   * <p>
   * The follower state is a standard Raft state in which the server receives replicated log entries from the leader.
   */
  FOLLOWER {
    @Override
    public RaftState createState(ServerStateController controller) {
      return new FollowerState(controller);
    }
  },

  /**
   * Represents the state of a server attempting to become the leader.
   * <p>
   * When a server in the follower state fails to receive communication from a valid leader for some time period,
   * the follower will transition to the candidate state. During this period, the candidate requests votes from
   * each of the other servers in the cluster. If the candidate wins the election by receiving votes from a majority
   * of the cluster, it will transition to the leader state.
   */
  CANDIDATE {
    @Override
    public RaftState createState(ServerStateController controller) {
      return new CandidateState(controller);
    }
  },

  /**
   * Represents the state of a server which is actively coordinating and replicating logs with other servers.
   * <p>
   * Leaders are responsible for handling and replicating writes from clients. Note that more than one leader can
   * exist at any given time, but Raft guarantees that no two leaders will exist for the same term.
   */
  LEADER {
    @Override
    public RaftState createState(ServerStateController controller) {
      return new LeaderState(controller);
    }
  }

}
