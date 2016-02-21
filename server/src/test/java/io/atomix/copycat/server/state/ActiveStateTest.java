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

import io.atomix.copycat.protocol.Response.Status;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.protocol.AppendRequest;
import io.atomix.copycat.server.protocol.PollRequest;
import io.atomix.copycat.server.protocol.VoteRequest;
import io.atomix.copycat.server.protocol.AppendResponse;
import io.atomix.copycat.server.protocol.PollResponse;
import io.atomix.copycat.server.protocol.VoteResponse;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

import static org.testng.Assert.*;

/**
 * Active state tests.
 */
@Test
public class ActiveStateTest extends AbstractStateTest<ActiveState> {

  @BeforeMethod
  @Override
  void beforeMethod() throws Throwable {
    super.beforeMethod();
    state = new ActiveState(serverContext) {
    };
  }

  @SuppressWarnings("unchecked")
  public void testAppendUpdatesLeaderAndTerm() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(1);
      AppendRequest request = AppendRequest.builder()
          .withTerm(2)
          .withLeader(members.get(1).hashCode())
          .withEntries(Collections.EMPTY_LIST)
          .withLogIndex(0)
          .withLogTerm(0)
          .withCommitIndex(0)
          .withGlobalIndex(0)
          .build();

      AppendResponse response = state.append(request).get();

      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(serverContext.getLeader(), members.get(1));
      assertEquals(serverContext.getLastVotedFor(), 0);
      assertEquals(response.term(), 2L);
      assertTrue(response.succeeded());
    });
  }

  @SuppressWarnings("unchecked")
  public void testAppendTermUpdatedAndTransitionedToFollower() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(1);
      AppendRequest request = AppendRequest.builder()
        .withTerm(2)
        .withEntries(Collections.EMPTY_LIST)
        .withCommitIndex(0)
        .withGlobalIndex(0)
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.succeeded());
      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(response.term(), 2L);
      assertEquals(serverContext.getState(), CopycatServer.State.FOLLOWER);
    });
  }

  public void testIncrementTermOnPoll() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(1);

      PollRequest request = PollRequest.builder()
        .withTerm(2)
        .withCandidate(serverContext.getCluster().member().id())
        .withLogIndex(1)
        .withLogTerm(1)
        .build();

      PollResponse response = state.poll(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.accepted());
      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

  public void testRejectPollOnTerm() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2);

      PollRequest request = PollRequest.builder()
        .withTerm(1)
        .withCandidate(serverContext.getCluster().member().id())
        .withLogIndex(1)
        .withLogTerm(1)
        .build();

      PollResponse response = state.poll(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.accepted());
      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

  public void testRejectPollOnLogIndex() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(1);
      append(3, 1);

      PollRequest request = PollRequest.builder()
        .withTerm(2)
        .withCandidate(serverContext.getCluster().member().id())
        .withLogIndex(1)
        .withLogTerm(1)
        .build();

      PollResponse response = state.poll(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.accepted());
      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

  public void testRejectPollOnLogTerm() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2);
      append(3, 1);
      append(1, 2);

      PollRequest request = PollRequest.builder()
        .withTerm(2)
        .withCandidate(serverContext.getCluster().member().id())
        .withLogIndex(4)
        .withLogTerm(1)
        .build();

      PollResponse response = state.poll(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.accepted());
      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

  public void testIncrementTermAndTransitionOnVote() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(1);

      VoteRequest request = VoteRequest.builder()
        .withTerm(2)
        .withCandidate(serverContext.getClusterState().getActiveMemberStates().iterator().next().getMember().id())
        .withLogIndex(1)
        .withLogTerm(1)
        .build();

      VoteResponse response = state.vote(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.voted());
      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(response.term(), 2L);
      assertEquals(serverContext.getState(), CopycatServer.State.FOLLOWER);
    });
  }

  public void testRejectVoteOnTerm() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2);

      VoteRequest request = VoteRequest.builder()
        .withTerm(1)
        .withCandidate(serverContext.getClusterState().getActiveMemberStates().iterator().next().getMember().id())
        .withLogIndex(1)
        .withLogTerm(1)
        .build();

      VoteResponse response = state.vote(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.voted());
      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

  public void testRejectVoteOnLogIndex() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(1);
      append(3, 1);

      VoteRequest request = VoteRequest.builder()
        .withTerm(2)
        .withCandidate(serverContext.getClusterState().getActiveMemberStates().iterator().next().getMember().id())
        .withLogIndex(1)
        .withLogTerm(1)
        .build();

      VoteResponse response = state.vote(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.voted());
      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

  public void testRejectVoteOnLogTerm() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2);
      append(3, 1);
      append(1, 2);

      VoteRequest request = VoteRequest.builder()
        .withTerm(2)
        .withCandidate(serverContext.getClusterState().getActiveMemberStates().iterator().next().getMember().id())
        .withLogIndex(4)
        .withLogTerm(1)
        .build();

      VoteResponse response = state.vote(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.voted());
      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

}
