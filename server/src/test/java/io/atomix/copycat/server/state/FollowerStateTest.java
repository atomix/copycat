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
import io.atomix.copycat.server.protocol.AppendRequest;
import io.atomix.copycat.server.protocol.PollRequest;
import io.atomix.copycat.server.protocol.VoteRequest;
import io.atomix.copycat.server.protocol.AppendResponse;
import io.atomix.copycat.server.protocol.PollResponse;
import io.atomix.copycat.server.protocol.VoteResponse;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collections;

/**
 * Follower state test.
 */
@Test
public class FollowerStateTest extends AbstractStateTest<FollowerState> {
  @BeforeMethod
  @Override
  void beforeMethod() throws Throwable {
    super.beforeMethod();
    state = new FollowerState(serverContext);
  }

  /**
   * Tests that a follower will accept a poll for many candidates.
   */
  public void testFollowerAcceptsPollForMultipleCandidatesPerTerm() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2);
      PollRequest request1 = PollRequest.builder()
          .withTerm(2)
          .withCandidate(members.get(1).hashCode())
          .withLogIndex(0)
          .withLogTerm(0)
          .build();

      PollResponse response = state.poll(request1).get();

      threadAssertEquals(response.status(), Status.OK);
      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.accepted());

      PollRequest request2 = PollRequest.builder()
          .withTerm(2)
          .withCandidate(members.get(2).hashCode())
          .withLogIndex(0)
          .withLogTerm(0)
          .build();

      response = state.poll(request2).get();

      threadAssertEquals(response.status(), Status.OK);
      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.accepted());
    });
  }

  /**
   * Tests that a follower rejects a poll when the candidate's log is not up to date.
   */
  public void testFollowerRejectsPollWhenLogNotUpToDate() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2).setLeader(0);
      append(10, 1);
      PollRequest request = PollRequest.builder()
          .withTerm(2)
          .withCandidate(members.get(1).hashCode())
          .withLogIndex(5)
          .withLogTerm(1)
          .build();

      PollResponse response = state.poll(request).get();

      threadAssertEquals(response.status(), Status.OK);
      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertFalse(response.accepted());
    });
  }

  /**
   * Tests a follower handling a vote request with a higher term.
   */
  public void testFollowerSetsTermOnPollRequest() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(1).setLeader(0);
      PollRequest request = PollRequest.builder()
          .withTerm(2)
          .withCandidate(members.get(1).hashCode())
          .withLogIndex(0)
          .withLogTerm(0)
          .build();

      PollResponse response = state.poll(request).get();

      threadAssertEquals(response.status(), Status.OK);
      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.accepted());
    });
  }

  /**
   * Tests that a follower rejects a poll request with a lower term.
   */
  public void testFollowerRejectsPollRequestWithLowerTerm() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2).setLeader(0);
      PollRequest request = PollRequest.builder()
          .withTerm(1)
          .withCandidate(members.get(1).hashCode())
          .withLogIndex(0)
          .withLogTerm(0)
          .build();

      PollResponse response = state.poll(request).get();

      threadAssertEquals(response.status(), Status.OK);
      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertFalse(response.accepted());
    });
  }

  /**
   * Tests that a follower accepts a poll when the candidate's log is up to date.
   */
  public void testFollowerAcceptsPollWhenLogUpToDate() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2);
      append(10, 1);
      PollRequest request = PollRequest.builder()
          .withTerm(2)
          .withCandidate(members.get(1).hashCode())
          .withLogIndex(11)
          .withLogTerm(2)
          .build();

      PollResponse response = state.poll(request).get();

      threadAssertEquals(response.status(), Status.OK);
      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.accepted());
    });
  }

  /**
   * Tests a follower handling a vote request.
   */
  public void testFollowerSetsTermOnVoteRequest() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(1);
      VoteRequest request = VoteRequest.builder()
          .withTerm(2)
          .withCandidate(members.get(1).hashCode())
          .withLogIndex(0)
          .withLogTerm(0)
          .build();

      VoteResponse response = state.vote(request).get();

      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.voted());
    });
  }

  /**
   * Tests that a follower rejects a vote request with a lesser term.
   */
  public void testFollowerRejectsVoteRequestWithLesserTerm() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2);
      VoteRequest request = VoteRequest.builder()
          .withTerm(1)
          .withCandidate(members.get(1).hashCode())
          .withLogIndex(0)
          .withLogTerm(0)
          .build();

      VoteResponse response = state.vote(request).get();

      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertFalse(response.voted());
    });
  }

  /**
   * Tests that a follower will only vote for one candidate.
   */
  public void testFollowerVotesForOneCandidatePerTerm() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2).setLeader(0);
      VoteRequest request1 = VoteRequest.builder()
          .withTerm(2)
          .withCandidate(members.get(1).hashCode())
          .withLogIndex(0)
          .withLogTerm(0)
          .build();

      VoteResponse response = state.vote(request1).get();

      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(serverContext.getLastVotedFor(), members.get(1).hashCode());
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.voted());

      VoteRequest request2 = VoteRequest.builder()
          .withTerm(2)
          .withCandidate(members.get(2).hashCode())
          .withLogIndex(0)
          .withLogTerm(0)
          .build();

      response = state.vote(request2).get();

      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(serverContext.getLastVotedFor(), members.get(1).hashCode());
      threadAssertEquals(response.term(), 2L);
      threadAssertFalse(response.voted());
    });
  }

  /**
   * Tests that a follower overrides a vote from a previous term when the term increases.
   */
  public void testFollowerOverridesVoteForNewTerm() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2).setLeader(0);
      VoteRequest request1 = VoteRequest.builder()
          .withTerm(2)
          .withCandidate(members.get(1).hashCode())
          .withLogIndex(0)
          .withLogTerm(0)
          .build();

      VoteResponse response1 = state.vote(request1).get();

      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(serverContext.getLastVotedFor(), members.get(1).hashCode());
      threadAssertEquals(response1.term(), 2L);
      threadAssertTrue(response1.voted());

      VoteRequest request2 = VoteRequest.builder()
          .withTerm(3)
          .withCandidate(members.get(2).hashCode())
          .withLogIndex(0)
          .withLogTerm(0)
          .build();

      VoteResponse response2 = state.vote(request2).get();

      threadAssertEquals(serverContext.getTerm(), 3L);
      threadAssertEquals(serverContext.getLastVotedFor(), members.get(2).hashCode());
      threadAssertEquals(response2.term(), 3L);
      threadAssertTrue(response2.voted());
    });

  }

  /**
   * Tests that a follower rejects a vote when the candidate's log is not up to date.
   */
  public void testFollowerRejectsVoteWhenLogNotUpToDate() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2).setLeader(0);
      append(10, 1);
      VoteRequest request = VoteRequest.builder()
          .withTerm(2)
          .withCandidate(members.get(1).hashCode())
          .withLogIndex(5)
          .withLogTerm(1)
          .build();

      VoteResponse response = state.vote(request).get();

      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertFalse(response.voted());
    });
  }

  /**
   * Tests that a follower accepts a vote when the candidate's log is up to date.
   */
  public void testFollowerAcceptsVoteWhenLogUpToDate() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2).setLeader(0);
      append(10, 1);
      VoteRequest request = VoteRequest.builder()
          .withTerm(2)
          .withCandidate(members.get(1).hashCode())
          .withLogIndex(11)
          .withLogTerm(2)
          .build();

      VoteResponse response = state.vote(request).get();

      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.voted());
    });
  }

  /**
   * Tests that a follower updates a leader on append request with new term.
   */
  @SuppressWarnings("unchecked")
  public void testFollowerUpdatesLeaderAndTermOnAppend() throws Throwable {
    runOnServer(() -> {
      serverContext.setTerm(2).setLeader(0);
      VoteRequest request1 = VoteRequest.builder()
          .withTerm(2)
          .withCandidate(members.get(1).hashCode())
          .withLogIndex(0)
          .withLogTerm(0)
          .build();

      VoteResponse response1 = state.vote(request1).get();

      threadAssertEquals(serverContext.getTerm(), 2L);
      threadAssertEquals(serverContext.getLastVotedFor(), members.get(1).hashCode());
      threadAssertEquals(response1.term(), 2L);
      threadAssertTrue(response1.voted());

      AppendRequest request2 = AppendRequest.builder()
          .withTerm(3)
          .withLeader(members.get(2).hashCode())
          .withEntries(Collections.EMPTY_LIST)
          .withLogIndex(0)
          .withLogTerm(0)
          .withCommitIndex(0)
          .withGlobalIndex(0)
          .build();

      AppendResponse response2 = state.append(request2).get();

      threadAssertEquals(serverContext.getTerm(), 3L);
      threadAssertEquals(serverContext.getLeader().serverAddress(), members.get(2).serverAddress());
      threadAssertEquals(serverContext.getLastVotedFor(), 0);
      threadAssertEquals(response2.term(), 3L);
      threadAssertTrue(response2.succeeded());
    });
  }

}
