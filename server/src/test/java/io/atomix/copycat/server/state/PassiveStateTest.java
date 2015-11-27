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

import io.atomix.copycat.client.request.*;
import io.atomix.copycat.client.response.*;
import io.atomix.copycat.client.response.Response.Status;
import io.atomix.copycat.server.TestStateMachine.TestCommand;
import io.atomix.copycat.server.TestStateMachine.TestQuery;
import io.atomix.copycat.server.request.*;
import io.atomix.copycat.server.response.*;
import io.atomix.copycat.server.storage.TestEntry;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.testng.Assert.*;

/**
 * Passive state tests.
 */
@Test
public class PassiveStateTest extends AbstractStateTest<PassiveState> {
  @BeforeMethod
  @Override
  void beforeMethod() throws Throwable {
    super.beforeMethod();
    state = new PassiveState(serverState);
  }

  public void testAccept() throws Throwable {
    runOnServer(() -> {
      AcceptRequest request = AcceptRequest.builder().withAddress(members.get(0).serverAddress()).withSession(1).build();
      AcceptResponse response = state.accept(request).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testKeepAlive() throws Throwable {
    runOnServer(() -> {
      KeepAliveRequest request = KeepAliveRequest.builder()
          .withCommandSequence(1)
          .withEventVersion(1)
          .withSession(1)
          .build();
      KeepAliveResponse response = state.keepAlive(request).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testApplyCommits() throws Throwable {
    runOnServer(() -> {
      state.applyCommits(3);

      // TODO more assertions
      threadAssertEquals(serverState.getCommitIndex(), 3L);
    });
  }

  public void testDoAppendEntries() throws Throwable {
    // TODO
  }

  public void testDoCheckPreviousEntry() throws Throwable {
    // TODO
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
      
      threadAssertEquals(serverState.getTerm(), 2L);
      threadAssertEquals(serverState.getLeader(), members.get(1).serverAddress());
      threadAssertEquals(serverState.getLastVotedFor(), 0);
      threadAssertEquals(response.term(), 2L);
      threadAssertTrue(response.succeeded());
    });
  }

  public void testAppendTermAndLeaderUpdated() throws Throwable {
    runOnServer(() -> {
      int leader = serverState.getCluster().getVotingMembers().iterator().next().id();
      serverState.setTerm(1);
      AppendRequest request = AppendRequest.builder()
        .withTerm(2)
        .withLeader(leader)
        .withCommitIndex(0)
        .withGlobalIndex(0)
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.succeeded());
      assertEquals(serverState.getTerm(), 2L);
      assertEquals(serverState.getLeader().hashCode(), leader);
      assertEquals(response.term(), 2L);
    });
  }

  public void testRejectAppendOnTerm() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(2);
      append(2, 2);

      AppendRequest request = AppendRequest.builder()
        .withTerm(1)
        .withLeader(serverState.getCluster().getVotingMembers().iterator().next().id())
        .withLogIndex(2)
        .withLogTerm(2)
        .withCommitIndex(0)
        .withGlobalIndex(0)
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.succeeded());
      assertEquals(response.term(), 2L);
      assertEquals(response.logIndex(), 2L);
    });
  }

  public void testRejectAppendOnMissingLogIndex() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(2);
      append(1, 2);

      AppendRequest request = AppendRequest.builder()
        .withTerm(2)
        .withLeader(serverState.getCluster().getVotingMembers().iterator().next().id())
        .withLogIndex(2)
        .withLogTerm(2)
        .withCommitIndex(0)
        .withGlobalIndex(0)
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.succeeded());
      assertEquals(response.term(), 2L);
      assertEquals(response.logIndex(), 1L);
    });
  }

  public void testRejectAppendOnSkippedLogIndex() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(1);
      serverState.getLog().skip(1);

      AppendRequest request = AppendRequest.builder()
        .withTerm(1)
        .withLeader(serverState.getCluster().getVotingMembers().iterator().next().id())
        .withLogIndex(1)
        .withLogTerm(1)
        .withCommitIndex(0)
        .withGlobalIndex(0)
        .withEntries(new TestEntry().setIndex(2).setTerm(1))
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.succeeded());
      assertEquals(response.term(), 1L);
      assertEquals(response.logIndex(), 0L);
    });
  }

  public void testRejectAppendOnInconsistentLogTerm() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(2);
      append(2, 1);

      AppendRequest request = AppendRequest.builder()
        .withTerm(2)
        .withLeader(serverState.getCluster().getVotingMembers().iterator().next().id())
        .withLogIndex(2)
        .withLogTerm(2)
        .withCommitIndex(0)
        .withGlobalIndex(0)
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertFalse(response.succeeded());
      assertEquals(response.term(), 2L);
      assertEquals(response.logIndex(), 1L);
    });
  }

  public void testAppendOnEmptyLog() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(1);

      AppendRequest request = AppendRequest.builder()
        .withTerm(1)
        .withLeader(serverState.getCluster().getVotingMembers().iterator().next().id())
        .withLogIndex(0)
        .withLogTerm(0)
        .withCommitIndex(0)
        .withGlobalIndex(0)
        .withEntries(new TestEntry().setIndex(1).setTerm(1))
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.succeeded());
      assertEquals(response.term(), 1L);
      assertEquals(response.logIndex(), 1L);

      assertEquals(serverState.getLog().length(), 1L);
      assertNotNull(get(1));
    });
  }

  public void testAppendOnNonEmptyLog() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(1);
      append(1, 1);

      AppendRequest request = AppendRequest.builder()
        .withTerm(1)
        .withLeader(serverState.getCluster().getVotingMembers().iterator().next().id())
        .withLogIndex(0)
        .withLogTerm(0)
        .withCommitIndex(0)
        .withGlobalIndex(0)
        .withEntries(new TestEntry().setIndex(2).setTerm(1))
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.succeeded());
      assertEquals(response.term(), 1L);
      assertEquals(response.logIndex(), 2L);

      assertEquals(serverState.getLog().length(), 2L);
      assertNotNull(get(2));
    });
  }

  public void testAppendOnPartiallyConflictingEntries() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(1);
      append(1, 1);
      append(2, 2);

      AppendRequest request = AppendRequest.builder()
        .withTerm(3)
        .withLeader(serverState.getCluster().getVotingMembers().iterator().next().id())
        .withLogIndex(1)
        .withLogTerm(1)
        .withCommitIndex(0)
        .withGlobalIndex(0)
        .withEntries(new TestEntry().setIndex(2).setTerm(2), new TestEntry().setIndex(3).setTerm(3), new TestEntry().setIndex(4).setTerm(3))
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.succeeded());
      assertEquals(response.term(), 3L);
      assertEquals(response.logIndex(), 4L);

      assertEquals(serverState.getLog().length(), 4L);
      assertEquals(get(2).getTerm(), 2L);
      assertEquals(get(3).getTerm(), 3L);
      assertEquals(get(4).getTerm(), 3L);
    });
  }

  public void testAppendSkippedEntries() throws Throwable {
    runOnServer(() -> {
      serverState.setTerm(1);
      append(1, 1);

      AppendRequest request = AppendRequest.builder()
        .withTerm(1)
        .withLeader(serverState.getCluster().getVotingMembers().iterator().next().id())
        .withLogIndex(1)
        .withLogTerm(1)
        .withCommitIndex(0)
        .withGlobalIndex(0)
        .withEntries(new TestEntry().setIndex(2).setTerm(1), new TestEntry().setIndex(4).setTerm(1))
        .build();

      AppendResponse response = state.append(request).get();

      assertEquals(response.status(), Status.OK);
      assertTrue(response.succeeded());
      assertEquals(response.term(), 1L);
      assertEquals(response.logIndex(), 4L);

      assertEquals(serverState.getLog().length(), 4L);
      assertNotNull(get(2));
      assertNull(get(3));
      assertNotNull(get(4));
    });
  }

  public void testCommandWithoutLeader() throws Throwable {
    runOnServer(() -> {
      CommandRequest request = CommandRequest.builder().withSession(1).withCommand(new TestCommand("test")).build();
      CommandResponse response = state.command(request).get();
      assertNoLeaderError(response);
    });
  }

  public void testJoinWithoutLeader() throws Throwable {
    runOnServer(() -> {
      JoinRequest request = JoinRequest.builder().withMember(members.get(0)).build();
      JoinResponse response = state.join(request).get();
      assertNoLeaderError(response);
    });
  }

  public void testLeaveWithoutLeader() throws Throwable {
    runOnServer(() -> {
      LeaveRequest request = LeaveRequest.builder().withMember(members.get(0)).build();
      LeaveResponse response = state.leave(request).get();
      assertNoLeaderError(response);
    });
  }

  public void testQueryWithoutLeader() throws Throwable {
    runOnServer(() -> {
      QueryRequest request = QueryRequest.builder().withSession(1).withQuery(new TestQuery()).build();
      QueryResponse response = state.query(request).get();
      assertNoLeaderError(response);
    });
  }

  public void testRegister() throws Throwable {
    runOnServer(() -> {
      RegisterRequest request = RegisterRequest.builder().withClient(UUID.randomUUID()).build();
      RegisterResponse response = state.register(request).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testUnregister() throws Throwable {
    runOnServer(() -> {
      UnregisterRequest request = UnregisterRequest.builder().withSession(1).build();
      UnregisterResponse response = state.unregister(request).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testConnect() throws Throwable {
    runOnServer(() -> {
      ConnectRequest request = ConnectRequest.builder().withSession(1).build();
      ConnectResponse response = state.connect(request, null).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testPoll() throws Throwable {
    runOnServer(() -> {
      PollRequest request = PollRequest.builder().withCandidate(1).withLogIndex(1).withLogTerm(1).withTerm(1).build();
      PollResponse response = state.poll(request).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testPublish() throws Throwable {
    runOnServer(() -> {
      PublishRequest request = PublishRequest.builder().withSession(1).withEventVersion(1).build();
      PublishResponse response = state.publish(request).get();
      assertIllegalMemberStateError(response);
    });
  }

  public void testVote() throws Throwable {
    runOnServer(() -> {
      VoteRequest request = VoteRequest.builder().withCandidate(1).withLogIndex(1).withLogTerm(1).withTerm(1).build();
      VoteResponse response = state.vote(request).get();
      assertIllegalMemberStateError(response);
    });
  }

}
