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

import io.atomix.catalyst.transport.Server;
import io.atomix.copycat.protocol.Response;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.protocol.AppendRequest;
import io.atomix.copycat.server.protocol.VoteRequest;
import io.atomix.copycat.server.protocol.AppendResponse;
import io.atomix.copycat.server.protocol.VoteResponse;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.testng.Assert.*;

/**
 * Candidate state tests.
 */
@Test
public class CandidateStateTest extends AbstractStateTest<CandidateState> {

  @BeforeMethod
  @Override
  void beforeMethod() throws Throwable {
    super.beforeMethod();
    state = new CandidateState(serverContext);
  }

  @SuppressWarnings("unchecked")
  public void testCandidateAppendAndTransitionOnTerm() throws Throwable {
    runOnServer(() -> {
      int leader = serverContext.getClusterState().getActiveMemberStates().iterator().next().getMember().id();
      serverContext.setTerm(1);
      AppendRequest request = AppendRequest.builder()
        .withTerm(2)
        .withLeader(leader)
        .withEntries(Collections.EMPTY_LIST)
        .withCommitIndex(0)
        .withGlobalIndex(0)
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Response.Status.OK);
      assertTrue(response.succeeded());
      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(serverContext.getLeader().hashCode(), leader);
      assertEquals(response.term(), 2L);
      assertEquals(serverContext.getState(), CopycatServer.State.FOLLOWER);
    });
  }

  public void testCandidateIncrementsTermVotesForSelfOnElection() throws Throwable {
    runOnServer(() -> {
      int self = serverContext.getCluster().member().id();
      serverContext.setTerm(2);

      state.startElection();

      assertEquals(serverContext.getTerm(), 3L);
      assertEquals(serverContext.getLastVotedFor(), self);
    });
  }

  public void testCandidateVotesForSelfOnRequest() throws Throwable {
    runOnServer(() -> {
      int self = serverContext.getCluster().member().id();
      serverContext.setTerm(2);

      state.startElection();

      assertEquals(serverContext.getTerm(), 3L);

      VoteRequest request = VoteRequest.builder()
        .withTerm(3)
        .withCandidate(self)
        .withLogIndex(0)
        .withLogTerm(0)
        .build();

      VoteResponse response = state.vote(request).get();

      assertEquals(response.status(), Response.Status.OK);
      assertTrue(response.voted());
      assertEquals(serverContext.getTerm(), 3L);
      assertEquals(serverContext.getLastVotedFor(), self);
      assertEquals(response.term(), 3L);
    });
  }

  public void testCandidateVotesAndTransitionsOnTerm() throws Throwable {
    runOnServer(() -> {
      int candidate = serverContext.getClusterState().getActiveMemberStates().iterator().next().getMember().id();
      serverContext.setTerm(1);

      state.startElection();

      assertEquals(serverContext.getTerm(), 2L);

      VoteRequest request = VoteRequest.builder()
        .withTerm(3)
        .withCandidate(candidate)
        .withLogTerm(0)
        .withLogIndex(0)
        .build();

      VoteResponse response = state.vote(request).get();

      assertEquals(response.status(), Response.Status.OK);
      assertTrue(response.voted());
      assertEquals(serverContext.getTerm(), 3L);
      assertEquals(serverContext.getLastVotedFor(), candidate);
      assertEquals(response.term(), 3L);
      assertEquals(serverContext.getState(), CopycatServer.State.FOLLOWER);
    });
  }

  public void testCandidateRejectsVoteAndTransitionsOnTerm() throws Throwable {
    runOnServer(() -> {
      int candidate = serverContext.getClusterState().getActiveMemberStates().iterator().next().getMember().id();
      serverContext.setTerm(1);

      append(2, 1);

      state.startElection();

      assertEquals(serverContext.getTerm(), 2L);

      VoteRequest request = VoteRequest.builder()
        .withTerm(3)
        .withCandidate(candidate)
        .withLogTerm(0)
        .withLogIndex(0)
        .build();

      VoteResponse response = state.vote(request).get();

      assertEquals(response.status(), Response.Status.OK);
      assertFalse(response.voted());
      assertEquals(serverContext.getTerm(), 3L);
      assertEquals(serverContext.getLastVotedFor(), 0);
      assertEquals(response.term(), 3L);
      assertEquals(serverContext.getState(), CopycatServer.State.FOLLOWER);
    });
  }

  @SuppressWarnings("unchecked")
  public void testCandidateTransitionsToLeaderOnElection() throws Throwable {
    serverContext.onStateChange(state -> {
      if (state == CopycatServer.State.LEADER)
        resume();
    });

    runOnServer(() -> {
      for (MemberState member : serverContext.getClusterState().getRemoteMemberStates()) {
        Server server = transport.server();
        server.listen(member.getMember().serverAddress(), c -> {
          c.handler(VoteRequest.class, (Function) request -> CompletableFuture.completedFuture(VoteResponse.builder()
            .withTerm(2)
            .withVoted(true)
            .build()));
        });
      }
    });

    runOnServer(() -> {
      int self = serverContext.getCluster().member().id();
      serverContext.setTerm(1);

      state.startElection();

      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(serverContext.getLastVotedFor(), self);
    });
    await(1000);
  }

  @SuppressWarnings("unchecked")
  public void testCandidateTransitionsToFollowerOnRejection() throws Throwable {
    serverContext.onStateChange(state -> {
      if (state == CopycatServer.State.FOLLOWER)
        resume();
    });

    runOnServer(() -> {
      for (MemberState member : serverContext.getClusterState().getRemoteMemberStates()) {
        Server server = transport.server();
        server.listen(member.getMember().serverAddress(), c -> {
          c.handler(VoteRequest.class, (Function) request -> CompletableFuture.completedFuture(VoteResponse.builder()
            .withTerm(2)
            .withVoted(false)
            .build()));
        });
      }
    });

    runOnServer(() -> {
      int self = serverContext.getCluster().member().id();
      serverContext.setTerm(1);

      state.startElection();

      assertEquals(serverContext.getTerm(), 2L);
      assertEquals(serverContext.getLastVotedFor(), self);
    });
    await(1000);
  }

}
