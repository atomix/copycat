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

import io.atomix.copycat.protocol.CommandRequest;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.TestStateMachine.TestCommand;
import io.atomix.copycat.server.protocol.VoteRequest;
import io.atomix.copycat.server.protocol.VoteResponse;
import io.atomix.copycat.server.storage.entry.CommandEntry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.UUID;

/**
 * Leader state test.
 */
@Test
public class LeaderStateTest extends AbstractStateTest<LeaderState> {
  LeaderState state;

  @BeforeMethod
  @Override
  void beforeMethod() throws Throwable {
    super.beforeMethod();
    state = new LeaderState(serverContext);
  }

  /**
   * Tests that a leader steps down when it receives a higher term.
   */
  public void testLeaderStepsDownAndVotesOnHigherTerm() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(1).setLeader(0);
      VoteRequest request = VoteRequest.builder()
          .withTerm(2)
          .withCandidate(members.get(1).hashCode())
          .withLogIndex(11)
          .withLogTerm(2)
          .build();

      VoteResponse response = state.vote(request).get();
      
      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(serverContext.getLastVotedFor(), members.get(1).hashCode());
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.voted());
      threadAssertEquals(serverContext.getState(), CopycatServer.State.FOLLOWER);
    });
  }
}
