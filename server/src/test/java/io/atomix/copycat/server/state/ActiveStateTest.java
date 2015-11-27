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

import io.atomix.copycat.client.response.Response.Status;
import io.atomix.copycat.server.RaftServer.State;
import io.atomix.copycat.server.request.AppendRequest;
import io.atomix.copycat.server.request.PollRequest;
import io.atomix.copycat.server.request.VoteRequest;
import io.atomix.copycat.server.response.AppendResponse;
import io.atomix.copycat.server.response.PollResponse;
import io.atomix.copycat.server.response.VoteResponse;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
    state = new ActiveState(serverState) {
    };
  }

  public void testAppendUpdatesLeaderAndTerm() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(1);
      AppendRequest request = AppendRequest.builder()
          .withTerm(2)
          .withLeader(members.get(1).hashCode())
          .withLogIndex(0)
          .withLogTerm(0)
          .withCommitIndex(0)
          .withGlobalIndex(0)
          .build();

      AppendResponse response = state.append(request).get();

      assertEquals(serverState.getTerm(), 2L);
      assertEquals(serverState.getLeader(), members.get(1));
      assertEquals(serverState.getLastVotedFor(), 0);
      assertEquals(response.term(), 2L);
      assertTrue(response.succeeded());
    });
  }

  public void testAppendTermUpdatedAndTransitionedToFollower() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(1);
      AppendRequest request = AppendRequest.builder()
        .withTerm(2)
        .withCommitIndex(0)
        .withGlobalIndex(0)
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.succeeded());
      assertEquals(serverState.getTerm(), 2L);
      assertEquals(response.term(), 2L);
      assertEquals(serverState.getState(), State.FOLLOWER);
    });
  }

  public void testIncrementTermOnPoll() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(1);

      PollRequest request = PollRequest.builder()
        .withTerm(2)
        .withCandidate(serverState.getCluster().getMember().serverAddress().hashCode())
        .withLogIndex(1)
        .withLogTerm(1)
        .build();

      PollResponse response = state.poll(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.accepted());
      assertEquals(serverState.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

  public void testRejectPollOnTerm() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(2);

      PollRequest request = PollRequest.builder()
        .withTerm(1)
        .withCandidate(serverState.getCluster().getMember().serverAddress().hashCode())
        .withLogIndex(1)
        .withLogTerm(1)
        .build();

      PollResponse response = state.poll(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.accepted());
      assertEquals(serverState.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

  public void testRejectPollOnLogIndex() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(1);
      append(3, 1);

      PollRequest request = PollRequest.builder()
        .withTerm(2)
        .withCandidate(serverState.getCluster().getMember().serverAddress().hashCode())
        .withLogIndex(1)
        .withLogTerm(1)
        .build();

      PollResponse response = state.poll(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.accepted());
      assertEquals(serverState.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

  public void testRejectPollOnLogTerm() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(2);
      append(3, 1);
      append(1, 2);

      PollRequest request = PollRequest.builder()
        .withTerm(2)
        .withCandidate(serverState.getCluster().getMember().serverAddress().hashCode())
        .withLogIndex(4)
        .withLogTerm(1)
        .build();

      PollResponse response = state.poll(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.accepted());
      assertEquals(serverState.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

  public void testIncrementTermAndTransitionOnVote() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(1);

      VoteRequest request = VoteRequest.builder()
        .withTerm(2)
        .withCandidate(serverState.getCluster().getRemoteMemberStates(RaftMemberType.ACTIVE).iterator().next().getMember().id())
        .withLogIndex(1)
        .withLogTerm(1)
        .build();

      VoteResponse response = state.vote(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.voted());
      assertEquals(serverState.getTerm(), 2L);
      assertEquals(response.term(), 2L);
      assertEquals(serverState.getState(), State.FOLLOWER);
    });
  }

  public void testRejectVoteOnTerm() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(2);

      VoteRequest request = VoteRequest.builder()
        .withTerm(1)
        .withCandidate(serverState.getCluster().getRemoteMemberStates(RaftMemberType.ACTIVE).iterator().next().getMember().id())
        .withLogIndex(1)
        .withLogTerm(1)
        .build();

      VoteResponse response = state.vote(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.voted());
      assertEquals(serverState.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

  public void testRejectVoteOnLogIndex() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(1);
      append(3, 1);

      VoteRequest request = VoteRequest.builder()
        .withTerm(2)
        .withCandidate(serverState.getCluster().getRemoteMemberStates(RaftMemberType.ACTIVE).iterator().next().getMember().id())
        .withLogIndex(1)
        .withLogTerm(1)
        .build();

      VoteResponse response = state.vote(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.voted());
      assertEquals(serverState.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

  public void testRejectVoteOnLogTerm() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(2);
      append(3, 1);
      append(1, 2);

      VoteRequest request = VoteRequest.builder()
        .withTerm(2)
        .withCandidate(serverState.getCluster().getRemoteMemberStates(RaftMemberType.ACTIVE).iterator().next().getMember().id())
        .withLogIndex(4)
        .withLogTerm(1)
        .build();

      VoteResponse response = state.vote(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.voted());
      assertEquals(serverState.getTerm(), 2L);
      assertEquals(response.term(), 2L);
    });
  }

}
